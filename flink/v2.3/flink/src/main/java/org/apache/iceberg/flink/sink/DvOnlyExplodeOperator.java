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

import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits a writer committable into the per-row records the resolve operator keys on and the files
 * that go straight to the aggregator, and seeds the primary key index with the rows the table
 * already holds.
 *
 * <p>Deletes and row locations are emitted on the main output so they can be shuffled by equality
 * key. The files are emitted on {@link #FILES_STREAM} with the unresolved deletes stripped, since
 * they need no shuffle and are committed as they are.
 *
 * <p>{@link org.apache.flink.streaming.api.connector.sink2.CommittableSummary} records are dropped:
 * {@link IcebergWriteAggregator} ignores incoming summaries and emits a single one of its own, so
 * the pre-commit topology is free to repartition.
 *
 * <p>The index is seeded from here rather than from the keyed operator downstream because keying by
 * equality values spreads the rows of a data file over every subtask: a keyed operator reading the
 * table itself would have to read every file on every subtask, whereas this operator is not keyed
 * and its instances can split the files between them.
 *
 * <p>Seeding runs while the first checkpoint barrier is being handled, which is what makes the
 * index complete before any delete resolves. Records emitted there reach the keyed operator ahead
 * of that same barrier, so by the time it resolves the checkpoint's deletes it has already seen
 * every seeded row from every instance — no completion signal is needed. Seeding must not run on
 * the first record instead: an instance that receives no data would never seed, and the deletes
 * covering the files it owns would silently find nothing to remove.
 *
 * <p>It is skipped after a restore because the index is part of the restored state.
 */
@Internal
class DvOnlyExplodeOperator extends AbstractStreamOperator<DvOnlyRecord>
    implements OneInputStreamOperator<CommittableMessage<SinkWriteResult>, DvOnlyRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyExplodeOperator.class);

  // The type must be stated explicitly: an OutputTag cannot capture a parameterized type from an
  // anonymous subclass, which would leave the side output typed as a raw CommittableMessage and
  // make it impossible to union with the resolved deletion vectors.
  static final OutputTag<CommittableMessage<SinkWriteResult>> FILES_STREAM =
      new OutputTag<>(
          "dv-only-files", CommittableMessageTypeInfo.of(SinkWriteResultSerializer::new));

  private static final ListStateDescriptor<Boolean> SEEDED_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexSeeded", Types.BOOLEAN);

  private final TableLoader tableLoader;
  private final String branch;
  private final Set<Integer> equalityFieldIds;

  private transient ListState<Boolean> seededState;
  private transient boolean seeded;

  DvOnlyExplodeOperator(TableLoader tableLoader, String branch, Set<Integer> equalityFieldIds) {
    this.tableLoader = tableLoader;
    this.branch = branch;
    this.equalityFieldIds = ImmutableSet.copyOf(equalityFieldIds);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    seededState = context.getOperatorStateStore().getListState(SEEDED_DESCRIPTOR);
    for (Boolean value : seededState.get()) {
      seeded = seeded || value;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    seededState.clear();
    seededState.add(seeded);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (!seeded) {
      seedIndex();
      seeded = true;
    }

    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<SinkWriteResult>> element) {
    CommittableMessage<SinkWriteResult> message = element.getValue();
    if (!(message instanceof CommittableWithLineage)) {
      return;
    }

    CommittableWithLineage<SinkWriteResult> lineage =
        (CommittableWithLineage<SinkWriteResult>) message;
    SinkWriteResult result = lineage.getCommittable();
    long checkpointId = lineage.getCheckpointId();

    for (SerializedEqualityValues key : result.deleteKeys()) {
      output.collect(new StreamRecord<>(DvOnlyRecord.delete(key, checkpointId)));
    }

    for (PkIndexEntry entry : result.indexEntries()) {
      output.collect(new StreamRecord<>(DvOnlyRecord.addRow(entry, checkpointId)));
    }

    // Keep the files and drop the payload that the resolve operator consumes.
    output.collect(
        FILES_STREAM,
        new StreamRecord<>(
            lineage.map(committable -> new SinkWriteResult(committable.writeResult()))));
  }

  /** Reports the rows the table already holds so that deletes can resolve against them. */
  private void seedIndex() {
    int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
    LOG.info("Seeding the primary key index on subtask {}/{}", subtaskIndex, parallelism);

    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    Table table = tableLoader.loadTable();
    new PkIndexBootstrap(table, branch, equalityFieldIds)
        .read(
            subtaskIndex,
            parallelism,
            entry -> output.collect(new StreamRecord<>(DvOnlyRecord.bootstrapRow(entry))));
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}
