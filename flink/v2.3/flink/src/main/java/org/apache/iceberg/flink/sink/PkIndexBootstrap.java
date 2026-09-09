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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the rows already committed to a branch and reports where each of them lives, which is how
 * the primary key index learns about data written before the job started.
 *
 * <p>Files are split across the parallel instances by a stable hash of their location, so each file
 * is read exactly once no matter how many instances take part. Rows already covered by a deletion
 * vector are skipped, since they are not live.
 */
@Internal
class PkIndexBootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(PkIndexBootstrap.class);

  private final Table table;
  private final String branch;
  private final Set<Integer> equalityFieldIds;
  private final Schema keySchema;
  private final Schema readSchema;
  private final StructLikeSerializer serializer = new StructLikeSerializer();
  private final DeleteLoader deleteLoader;

  PkIndexBootstrap(Table table, String branch, Set<Integer> equalityFieldIds) {
    Preconditions.checkArgument(
        equalityFieldIds != null && !equalityFieldIds.isEmpty(),
        "Equality field ids must not be empty");
    this.table = table;
    this.branch = branch;
    this.equalityFieldIds = ImmutableSet.copyOf(equalityFieldIds);
    this.keySchema = TypeUtil.select(table.schema(), this.equalityFieldIds);
    Preconditions.checkArgument(
        TypeUtil.getProjectedIds(keySchema).containsAll(this.equalityFieldIds),
        "Equality field ids %s are not present in table schema",
        this.equalityFieldIds);
    this.readSchema = withRowPosition(keySchema);
    this.deleteLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
  }

  /**
   * Reports every live row of the files assigned to {@code subtaskIndex}.
   *
   * @param subtaskIndex index of the calling instance
   * @param parallelism number of instances sharing the work
   * @param out receives one entry per live row
   */
  void read(int subtaskIndex, int parallelism, Consumer<PkIndexEntry> out) {
    Snapshot snapshot = table.snapshot(branch);
    if (snapshot == null) {
      LOG.info("Branch '{}' of table {} is empty, nothing to bootstrap", branch, table.name());
      return;
    }

    long files = 0;
    long rows = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table
            .newScan()
            .useSnapshot(snapshot.snapshotId())
            .planWith(ThreadPools.getWorkerPool())
            .planFiles()) {
      for (FileScanTask task : tasks) {
        if (!ownedBy(task, subtaskIndex, parallelism)) {
          continue;
        }

        files++;
        rows += read(task, out);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to plan files for the primary key index of table " + table.name(), e);
    }

    LOG.info(
        "Bootstrapped {} live row(s) from {} data file(s) of branch '{}' on subtask {}/{}",
        rows,
        files,
        branch,
        subtaskIndex,
        parallelism);
  }

  /** Spreads the files over the parallel instances so that each file is read exactly once. */
  private static boolean ownedBy(FileScanTask task, int subtaskIndex, int parallelism) {
    return Math.floorMod(task.file().location().hashCode(), parallelism) == subtaskIndex;
  }

  private long read(FileScanTask task, Consumer<PkIndexEntry> out) throws IOException {
    ContentFile<?> file = task.file();
    PositionDeleteIndex deleted = deletedPositions(task);
    Types.StructType partitionType = task.spec().partitionType();
    byte[] partition = serializer.encodePartition(file.partition(), partitionType);

    InputFile input = table.io().newInputFile(file.location());
    ReadBuilder<Record, Schema> builder =
        FormatModelRegistry.readBuilder(file.format(), Record.class, input);
    long rows = 0;
    try (CloseableIterable<Record> records =
        builder.project(readSchema).reuseContainers().build()) {
      Accessor<StructLike> positionAccessor =
          readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
      for (Record record : records) {
        long position = (long) positionAccessor.get(record);
        if (deleted != null && deleted.isDeleted(position)) {
          continue;
        }

        SerializedEqualityValues key = serializer.serializeKey(record, keySchema.asStruct());
        out.accept(
            new PkIndexEntry(
                key,
                new DVPosition(
                    file.location(),
                    position,
                    file.specId(),
                    partition,
                    PkIndexEntry.UNKNOWN_SEQUENCE)));
        rows++;
      }
    }

    return rows;
  }

  /** Positions of the file that a deletion vector already removed, or null when there is none. */
  private PositionDeleteIndex deletedPositions(FileScanTask task) {
    List<DeleteFile> deletionVectors = Lists.newArrayList();
    for (DeleteFile deleteFile : task.deletes()) {
      if (ContentFileUtil.isDV(deleteFile)) {
        deletionVectors.add(deleteFile);
      } else {
        // The sink refuses to start when the branch holds equality deletes, and a V3 table has no
        // standalone position deletes, so anything else means the table is not in the shape this
        // write path assumes.
        throw new IllegalStateException(
            String.format(
                "Cannot build the primary key index of table %s: data file %s has a %s delete file "
                    + "(%s) attached, but only deletion vectors are supported.",
                table.name(),
                task.file().location(),
                deleteFile.content() == FileContent.EQUALITY_DELETES ? "equality" : "position",
                deleteFile.location()));
      }
    }

    if (deletionVectors.isEmpty()) {
      return null;
    }

    return deleteLoader.loadPositionDeletes(deletionVectors, task.file().location());
  }

  private static Schema withRowPosition(Schema schema) {
    List<Types.NestedField> columns = Lists.newArrayList(schema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    return new Schema(columns);
  }
}
