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

import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves equality deletes against a primary key index, so that a delete can be expressed as a
 * position in the data file that holds the row. Keyed by the serialized equality values, so every
 * record for one key is handled by the same subtask.
 *
 * <p>The index keeps the location of the live row (or rows, if the source produced duplicates) for
 * each key. Records arriving during a checkpoint are accumulated per key and applied when the
 * checkpoint barrier arrives, in two phases: deletes resolve against the index first, then the rows
 * written during the checkpoint enter it. Applying them in this order is what lets a key that was
 * deleted and re-inserted in the same checkpoint survive, and it removes the need for the
 * event-time timers the post-commit converter relies on.
 *
 * <p>The accumulated records are held in memory rather than in keyed state: the barrier drains them
 * before the state snapshot is taken, so they are always empty when the snapshot happens and a
 * restore replays them from the writer. Memory therefore scales with the number of distinct keys
 * changed per checkpoint, not with the table size. The index itself is keyed state and is backed by
 * the configured state backend.
 */
@Internal
class DvOnlyResolveOperator extends AbstractStreamOperator<DVPosition>
    implements OneInputStreamOperator<DvOnlyRecord, DVPosition> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyResolveOperator.class);

  private static final ListStateDescriptor<DVPosition> LIVE_POSITIONS_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyLivePositions", TypeInformation.of(DVPosition.class));

  /** Location of the live rows of one key. */
  private transient ListState<DVPosition> livePositions;

  /** Changes accumulated for the current checkpoint, keyed by equality values. */
  private transient Map<SerializedEqualityValues, PendingChange> pending;

  @Override
  public void open() throws Exception {
    super.open();
    livePositions = getRuntimeContext().getListState(LIVE_POSITIONS_DESCRIPTOR);
    pending = Maps.newLinkedHashMap();
  }

  @Override
  public void processElement(StreamRecord<DvOnlyRecord> element) throws Exception {
    DvOnlyRecord record = element.getValue();
    switch (record.type()) {
      case DELETE -> changeFor(record).deleted = true;
      case ADD_ROW -> changeFor(record).added.add(record.position());
        // Already committed when the job started, so it is part of the index right away and can be
        // resolved by a delete in the current checkpoint.
      case BOOTSTRAP_ROW -> livePositions.add(record.position());
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    int resolved = 0;
    for (Map.Entry<SerializedEqualityValues, PendingChange> entry : pending.entrySet()) {
      // The barrier is not delivered per key, so the key context has to be set explicitly before
      // touching the keyed index.
      setCurrentKey(entry.getKey());
      resolved += apply(entry.getValue());
    }

    if (!pending.isEmpty()) {
      LOG.debug(
          "Resolved {} deleted row(s) from {} changed key(s) for checkpoint {}",
          resolved,
          pending.size(),
          checkpointId);
    }

    pending.clear();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  /**
   * Applies the changes of one key: a delete removes the rows currently indexed and reports their
   * positions, then the rows written during this checkpoint are indexed. Returns the number of rows
   * reported as deleted.
   */
  private int apply(PendingChange change) throws Exception {
    int resolved = 0;
    if (change.deleted) {
      for (DVPosition position : livePositions.get()) {
        output.collect(new StreamRecord<>(position));
        resolved++;
      }

      livePositions.clear();
    }

    for (DVPosition position : change.added) {
      livePositions.add(position);
    }

    return resolved;
  }

  private PendingChange changeFor(DvOnlyRecord record) {
    return pending.computeIfAbsent(record.key(), key -> new PendingChange());
  }

  /** Net effect accumulated for one key within a checkpoint. */
  private static final class PendingChange {
    private final List<DVPosition> added = Lists.newArrayList();
    private boolean deleted;
  }
}
