/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns resolved row positions into deletion vectors. Keyed by the path of the data file the
 * positions belong to, so that a data file is handled by a single subtask and ends up with a single
 * deletion vector per commit.
 *
 * <p>A data file may carry at most one deletion vector, so an existing one has to be merged into
 * the new one and dropped in the same commit. The vector to merge is either the one this operator
 * wrote for the file earlier, which is remembered in keyed state because the commit that carries it
 * may still be in flight, or the one currently in the table.
 *
 * <p>Positions are buffered in memory and written when the checkpoint barrier arrives, which is
 * what makes the resulting files part of the same commit as the deletes that produced them. The
 * buffer is empty whenever a snapshot is taken, so a restore replays it from upstream.
 */
@Internal
class DvOnlyDVWriterOperator extends AbstractStreamOperator<CommittableMessage<SinkWriteResult>>
    implements OneInputStreamOperator<DVPosition, CommittableMessage<SinkWriteResult>> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyDVWriterOperator.class);

  private static final ValueStateDescriptor<DeleteFile> LAST_DV_DESCRIPTOR =
      new ValueStateDescriptor<>("dvOnlyLastWrittenDv", TypeInformation.of(DeleteFile.class));

  private final TableLoader tableLoader;
  private final String branch;

  /** Deletion vector this operator last wrote for the current data file, committed or not. */
  private transient ValueState<DeleteFile> lastWrittenDv;

  private transient Table table;
  private transient OutputFileFactory fileFactory;
  private transient DeleteLoader deleteLoader;
  private transient Map<String, FilePositions> buffered;
  private transient int subtaskId;

  DvOnlyDVWriterOperator(TableLoader tableLoader, String branch) {
    this.tableLoader = tableLoader;
    this.branch = branch;
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();
    subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    fileFactory =
        OutputFileFactory.builderFor(table, subtaskId, 0L).format(FileFormat.PUFFIN).build();
    deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
    lastWrittenDv = getRuntimeContext().getState(LAST_DV_DESCRIPTOR);
    buffered = Maps.newLinkedHashMap();
  }

  @Override
  public void processElement(StreamRecord<DVPosition> element) {
    DVPosition position = element.getValue();
    buffered
        .computeIfAbsent(
            position.dataFilePath(),
            path -> new FilePositions(position.specId(), position.partition()))
        .positions
        .addLong(position.position());
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (!buffered.isEmpty()) {
      writeDeletionVectors(checkpointId);
      buffered.clear();
    }

    super.prepareSnapshotPreBarrier(checkpointId);
  }

  private void writeDeletionVectors(long checkpointId) throws Exception {
    table.refresh();
    Map<String, DeleteFile> previous = previousDeletionVectors();

    BaseDVFileWriter writer =
        new BaseDVFileWriter(fileFactory, path -> loadPrevious(path, previous));
    try (BaseDVFileWriter closeable = writer) {
      for (Map.Entry<String, FilePositions> entry : buffered.entrySet()) {
        String dataFilePath = entry.getKey();
        FilePositions filePositions = entry.getValue();
        PartitionSpec spec = table.specs().get(filePositions.specId);
        StructLike partition = filePositions.partition(spec.partitionType());
        filePositions.positions.forEach(
            (long position) -> closeable.delete(dataFilePath, position, spec, partition));
      }
    }

    DeleteWriteResult result = writer.result();
    for (DeleteFile dv : result.deleteFiles()) {
      // Remember the vector per data file so that the next checkpoint merges it even when the
      // commit carrying it has not landed yet.
      setCurrentKey(dv.referencedDataFile());
      lastWrittenDv.update(dv);
    }

    LOG.info(
        "Wrote {} deletion vector(s) covering {} data file(s), superseding {}, for checkpoint {}",
        result.deleteFiles().size(),
        buffered.size(),
        result.rewrittenDeleteFiles().size(),
        checkpointId);

    WriteResult writeResult =
        WriteResult.builder()
            .addDeleteFiles(result.deleteFiles())
            .addRewrittenDeleteFiles(result.rewrittenDeleteFiles())
            .addReferencedDataFiles(result.referencedDataFiles())
            .build();
    output.collect(
        new StreamRecord<>(
            new CommittableWithLineage<>(
                new SinkWriteResult(writeResult), checkpointId, subtaskId)));
  }

  /**
   * Deletion vector currently covering each buffered data file: the one this operator wrote if
   * there is one, otherwise the one in the table.
   */
  private Map<String, DeleteFile> previousDeletionVectors() throws IOException {
    Map<String, DeleteFile> previous = Maps.newHashMap();
    Set<String> unknown = Sets.newHashSet();
    for (String dataFilePath : buffered.keySet()) {
      setCurrentKey(dataFilePath);
      DeleteFile written = lastWrittenDv.value();
      if (written != null) {
        previous.put(dataFilePath, written);
      } else {
        unknown.add(dataFilePath);
      }
    }

    if (!unknown.isEmpty()) {
      previous.putAll(committedDeletionVectors(unknown));
    }

    return previous;
  }

  /** Looks up the deletion vectors the branch currently holds for the given data files. */
  private Map<String, DeleteFile> committedDeletionVectors(Set<String> dataFilePaths) {
    Map<String, DeleteFile> found = Maps.newHashMap();
    Snapshot snapshot = table.snapshot(branch);
    if (snapshot == null) {
      return found;
    }

    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          if (ContentFileUtil.isDV(deleteFile)
              && dataFilePaths.contains(deleteFile.referencedDataFile())) {
            found.put(deleteFile.referencedDataFile(), deleteFile);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
      }
    }

    return found;
  }

  private PositionDeleteIndex loadPrevious(String dataFilePath, Map<String, DeleteFile> previous) {
    DeleteFile deleteFile = previous.get(dataFilePath);
    if (deleteFile == null) {
      return null;
    }

    return deleteLoader.loadPositionDeletes(ImmutableList.of(deleteFile), dataFilePath);
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  /** Positions buffered for one data file, with the spec and partition needed to write them. */
  private static final class FilePositions {
    private final int specId;
    private final byte[] encodedPartition;
    private final Roaring64Bitmap positions = new Roaring64Bitmap();
    private StructLike decodedPartition;

    private FilePositions(int specId, byte[] encodedPartition) {
      this.specId = specId;
      this.encodedPartition = encodedPartition;
    }

    private StructLike partition(Types.StructType partitionType) {
      if (decodedPartition == null) {
        decodedPartition = StructLikeSerializer.decodePartition(encodedPartition, partitionType);
      }

      return decodedPartition;
    }
  }
}
