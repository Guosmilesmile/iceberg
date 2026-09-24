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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks which equality keys the primary key index holds in each data file, and reads the files it
 * is asked to index. Keyed by data file path, so a file is owned by a single subtask.
 *
 * <p>The index resolves a delete into the position of the row it removes, which stops being valid
 * once the data file holding that row leaves the table. This operator is what makes those positions
 * removable: every row that enters the index passes through here first, so when {@link
 * DvOnlyCoordinator} reports a file as gone, the keys that have to forget it are known without
 * rebuilding the index.
 *
 * <p>The mapping may name a key whose positions in the file were already consumed by an earlier
 * delete. Dropping such a key is a no-op, whereas missing one would leave a position pointing at a
 * file that is gone, so the mapping is allowed to be generous but never short. It is discarded as a
 * whole once the file leaves the table, which is the only point at which entries stop being useful.
 */
@Internal
class DvOnlyFileIndexOperator extends AbstractStreamOperator<DvOnlyRecord>
    implements OneInputStreamOperator<DvOnlyRecord, DvOnlyRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyFileIndexOperator.class);

  private static final ListStateDescriptor<SerializedEqualityValues> KEYS_DESCRIPTOR =
      new ListStateDescriptor<>(
          "dvOnlyFileKeys", TypeInformation.of(SerializedEqualityValues.class));

  private final TableLoader tableLoader;
  private final Set<Integer> equalityFieldIds;

  /** Keys the index holds positions for in the current data file. */
  private transient ListState<SerializedEqualityValues> keys;

  private transient PkIndexFileReader reader;

  DvOnlyFileIndexOperator(TableLoader tableLoader, Set<Integer> equalityFieldIds) {
    this.tableLoader = tableLoader;
    this.equalityFieldIds = ImmutableSet.copyOf(equalityFieldIds);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    Table table = tableLoader.loadTable();
    reader = new PkIndexFileReader(table, equalityFieldIds);
    keys = getRuntimeContext().getListState(KEYS_DESCRIPTOR);
  }

  @Override
  public void processElement(StreamRecord<DvOnlyRecord> element) throws Exception {
    DvOnlyRecord record = element.getValue();
    switch (record.type()) {
      case ADD_ROW -> {
        keys.add(record.key());
        output.collect(element);
      }
      case READ_FILE -> indexFile(record);
      case DROP_FILE -> dropFile(record);
      default -> output.collect(element);
    }
  }

  /** Reports the live rows of a data file and remembers the keys they belong to. */
  private void indexFile(DvOnlyRecord record) {
    long rows =
        reader.read(
            PkIndexReadTask.decode(record.readTask()),
            entry -> {
              register(entry.key());
              output.collect(new StreamRecord<>(DvOnlyRecord.bootstrapRow(entry)));
            });

    LOG.debug("Indexed {} live row(s) of data file {}", rows, record.filePath());
  }

  /** Asks every key of a data file that left the table to forget the positions it held there. */
  private void dropFile(DvOnlyRecord record) throws Exception {
    Iterable<SerializedEqualityValues> indexed = keys.get();
    if (indexed == null) {
      return;
    }

    long dropped = 0;
    for (SerializedEqualityValues key : indexed) {
      output.collect(new StreamRecord<>(DvOnlyRecord.dropPositions(key, record.filePath())));
      dropped++;
    }

    if (dropped > 0) {
      keys.clear();
      LOG.debug("Dropped {} key(s) indexed in data file {}", dropped, record.filePath());
    }
  }

  private void register(SerializedEqualityValues key) {
    try {
      keys.add(key);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to record the keys of a data file", e);
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}
