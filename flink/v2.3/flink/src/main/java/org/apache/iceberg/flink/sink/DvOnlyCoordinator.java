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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the primary key index in step with the table by deciding, once per checkpoint, which data
 * files have to be indexed and which ones the index must forget. Runs with a parallelism of one so
 * that a single view of the table drives every shard of the index.
 *
 * <p>The index has to be complete before a delete can resolve against it, so the first checkpoint
 * requests every data file of the branch. From then on only what changed is requested: the
 * coordinator remembers the snapshot the index reflects and inspects the snapshots committed since.
 *
 * <p>Only snapshots that <em>remove</em> data files matter. The sink adds data files and deletion
 * vectors but never removes a data file, so such a snapshot was produced by something else,
 * typically a compaction. The positions the index holds in the removed files are stale and the
 * replacements have to be read, which is what keeps compaction from breaking the index without
 * rebuilding it from scratch. Snapshots that only add data files are left alone: the rows this sink
 * wrote are reported by the writer itself, and rows written by another job are out of scope.
 *
 * <p>Commands are emitted while the checkpoint barrier is handled, so they reach the index ahead of
 * that barrier and the deletes of the same checkpoint already resolve against the updated index.
 * They are idempotent, which is what makes a replay after a failed checkpoint harmless: indexing a
 * file twice yields the same positions and dropping a file twice is a no-op.
 */
@Internal
class DvOnlyCoordinator extends AbstractStreamOperator<DvOnlyRecord>
    implements OneInputStreamOperator<CommittableMessage<SinkWriteResult>, DvOnlyRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyCoordinator.class);

  private static final ListStateDescriptor<Boolean> BUILT_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexBuilt", Types.BOOLEAN);

  private static final ListStateDescriptor<Long> COVERAGE_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexCoverage", Types.LONG);

  private static final ListStateDescriptor<String> KEY_FINGERPRINT_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexKeyFingerprint", Types.STRING);

  private final TableLoader tableLoader;
  private final String branch;
  private final String keyFingerprint;

  private transient Table table;
  private transient ListState<Boolean> builtState;
  private transient ListState<Long> coverageState;
  private transient ListState<String> fingerprintState;

  /** Whether the index holds the rows the table had when this job first ran. */
  private transient boolean built;

  /** Snapshot the index reflects, or null while the branch has none. */
  private transient Long coverage;

  DvOnlyCoordinator(TableLoader tableLoader, String branch, String keyFingerprint) {
    this.tableLoader = tableLoader;
    this.branch = branch;
    this.keyFingerprint = keyFingerprint;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    builtState = context.getOperatorStateStore().getListState(BUILT_DESCRIPTOR);
    for (Boolean value : builtState.get()) {
      built = built || value;
    }

    coverageState = context.getOperatorStateStore().getListState(COVERAGE_DESCRIPTOR);
    for (Long value : coverageState.get()) {
      coverage = value;
    }

    fingerprintState = context.getOperatorStateStore().getListState(KEY_FINGERPRINT_DESCRIPTOR);
    String restoredFingerprint = null;
    for (String value : fingerprintState.get()) {
      restoredFingerprint = value;
    }

    if (restoredFingerprint != null) {
      // The index is keyed by serialized equality values, so an index built from other equality
      // fields than the ones currently configured can no longer match them.
      Preconditions.checkState(
          restoredFingerprint.equals(keyFingerprint),
          "The primary key index was built from equality fields [%s], but the sink is configured "
              + "with [%s]. Clear the job state and restart to rebuild the index.",
          restoredFingerprint,
          keyFingerprint);
    } else if (built) {
      LOG.warn("Restored a primary key index without a key fingerprint, skipping the check");
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    builtState.clear();
    builtState.add(built);

    coverageState.clear();
    if (coverage != null) {
      coverageState.add(coverage);
    }

    fingerprintState.clear();
    fingerprintState.add(keyFingerprint);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<SinkWriteResult>> element) {
    // The committables travel to the aggregator through the other branch of the topology. This
    // operator only consumes the stream to be driven by its checkpoint barriers.
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    table.refresh();
    Snapshot head = table.snapshot(branch);
    if (built) {
      indexExternalChanges(head);
    } else {
      indexBranch(head);
      built = true;
    }

    coverage = head != null ? head.snapshotId() : null;
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  /** Requests indexing of every data file of the branch, which is how the index starts out. */
  private void indexBranch(Snapshot head) {
    if (head == null) {
      LOG.info("Branch '{}' of table {} is empty, nothing to index", branch, table.name());
      return;
    }

    long files = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table
            .newScan()
            .useSnapshot(head.snapshotId())
            .planWith(ThreadPools.getWorkerPool())
            .planFiles()) {
      for (FileScanTask task : tasks) {
        requestRead(task.file(), task.deletes());
        files++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to plan files for the primary key index of table " + table.name(), e);
    }

    LOG.info(
        "Requested indexing of {} data file(s) from snapshot {} of branch '{}' of table {}",
        files,
        head.snapshotId(),
        branch,
        table.name());
  }

  /** Replaces the index entries of data files that another operation rewrote or removed. */
  private void indexExternalChanges(Snapshot head) {
    if (head == null) {
      return;
    }

    long removed = 0;
    long added = 0;
    for (Snapshot snapshot : snapshotsSince(head)) {
      if (PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.DELETED_FILES_PROP, 0)
          == 0) {
        continue;
      }

      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      for (DataFile file : changes.removedDataFiles()) {
        output.collect(new StreamRecord<>(DvOnlyRecord.dropFile(file.location())));
        removed++;
      }

      // The replacements hold the rows that survived the rewrite, including the ones the index
      // just lost. Their deletion vectors, if any, are picked up by the reader as it goes.
      for (DataFile file : changes.addedDataFiles()) {
        requestRead(file, ImmutableList.of());
        added++;
      }
    }

    if (removed > 0 || added > 0) {
      LOG.info(
          "Dropped {} data file(s) from the primary key index and requested indexing of {} "
              + "replacement(s) on branch '{}' of table {}",
          removed,
          added,
          branch,
          table.name());
    }
  }

  /** Snapshots committed since the index was last updated, oldest first. */
  private List<Snapshot> snapshotsSince(Snapshot head) {
    List<Snapshot> pending = Lists.newArrayList();
    Snapshot current = head;
    while (current != null && !Objects.equals(current.snapshotId(), coverage)) {
      pending.add(current);
      Long parentId = current.parentId();
      current = parentId != null ? table.snapshot(parentId) : null;
    }

    Preconditions.checkState(
        coverage == null || current != null,
        "Snapshot %s that the primary key index of table %s was built from is no longer an "
            + "ancestor of branch '%s'. Clear the job state and restart to rebuild the index.",
        coverage,
        table.name(),
        branch);

    Collections.reverse(pending);
    return pending;
  }

  private void requestRead(DataFile file, List<DeleteFile> deletes) {
    output.collect(
        new StreamRecord<>(
            DvOnlyRecord.readFile(file.location(), new PkIndexReadTask(file, deletes).encode())));
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}
