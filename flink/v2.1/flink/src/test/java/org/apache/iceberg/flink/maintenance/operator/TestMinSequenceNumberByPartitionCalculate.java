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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

public class TestMinSequenceNumberByPartitionCalculate extends OperatorTestBase {

  @Test
  void testCalculateMinSequence() throws Exception {
    Table table = createPartitionedTable();

    try (KeyedOneInputStreamOperatorTestHarness<
            DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>
        harness = createHarness(table)) {
      harness.open();

      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 10L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 9L), 1));

      harness.processWatermark(1);

      assertThat(harness.extractOutputValues()).hasSize(1);
      assertThat(harness.extractOutputValues().get(0).sequenceNumber()).isEqualTo(9L);
    }
  }

  @Test
  void testCalculateMinSequenceForEachPartition() throws Exception {
    Table table = createPartitionedTable();

    try (KeyedOneInputStreamOperatorTestHarness<
            DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>
        harness = createHarness(table)) {
      harness.open();

      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 10L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 9L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p2", 20L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p2", 5L), 1));

      harness.processWatermark(1);

      assertThat(harness.extractOutputValues())
          .extracting(
              TestMinSequenceNumberByPartitionCalculate::partition, DeleteFileInfo::sequenceNumber)
          .containsExactlyInAnyOrder(tuple("p1", 9L), tuple("p2", 5L));
    }
  }

  @Test
  void testCalculateMinSequenceAfterStateIsCleared() throws Exception {
    Table table = createPartitionedTable();

    try (KeyedOneInputStreamOperatorTestHarness<
            DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>
        harness = createHarness(table)) {
      harness.open();

      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 10L), 1));
      harness.processWatermark(1);

      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 12L), 2));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 8L), 2));
      harness.processWatermark(2);

      assertThat(harness.extractOutputValues())
          .extracting(DeleteFileInfo::sequenceNumber)
          .containsExactly(10L, 8L);
    }
  }

  @Test
  void testCalculateMinSequenceAfterRestoreFromState() throws Exception {
    Table table = createPartitionedTable();
    OperatorSubtaskState state;

    try (KeyedOneInputStreamOperatorTestHarness<
            DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>
        harness = createHarness(table)) {
      harness.open();

      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 10L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p1", 7L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p2", 20L), 1));
      harness.processElement(new StreamRecord<>(deleteFileInfo(table, "p2", 15L), 1));

      state = harness.snapshot(1L, 1L);
    }

    try (KeyedOneInputStreamOperatorTestHarness<
            DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>
        harness = createHarness(table)) {
      harness.initializeState(state);
      harness.open();

      harness.processWatermark(1);

      assertThat(harness.extractOutputValues())
          .extracting(
              TestMinSequenceNumberByPartitionCalculate::partition, DeleteFileInfo::sequenceNumber)
          .containsExactlyInAnyOrder(tuple("p1", 7L), tuple("p2", 15L));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>
      createHarness(Table table) throws Exception {
    RowType deleteFilePartitionRowType = DeleteFilePartitionKey.partitionRowType(table);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(
            new MinSequenceNumberByPartitionCalculate(deleteFilePartitionRowType)),
        DeleteFilePartitionKey.selector(deleteFilePartitionRowType),
        TypeInformation.of(DeleteFilePartitionKey.class));
  }

  private static String partition(DeleteFileInfo info) {
    return info.partition().getString(0).toString();
  }

  private static DeleteFileInfo deleteFileInfo(Table table, String partition, long sequenceNumber) {
    return new DeleteFileInfo(
        partition == null ? null : GenericRowData.of(StringData.fromString(partition)),
        table.spec().specId(),
        sequenceNumber);
  }
}
