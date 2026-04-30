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

import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.RemoveDanglingDeletes;
import org.junit.jupiter.api.Test;

public class TestDanglingDvsDetector extends OperatorTestBase {

  private static final String DATA_FILE_PATH = "s3://bucket/table/data-file.parquet";
  private static final String OTHER_DATA_FILE_PATH = "s3://bucket/table/other-data-file.parquet";
  private static final String DV_FILE_PATH = "s3://bucket/table/dv-file.puffin";
  private static final String OTHER_DV_FILE_PATH = "s3://bucket/table/other-dv-file.puffin";
  private static final String PARTITION = "p1";
  private static final long RECORD_COUNT = 1L;
  private static final long FILE_SIZE_IN_BYTES = 10L;
  private static final long CONTENT_OFFSET = 4L;
  private static final long CONTENT_SIZE_IN_BYTES = 6L;

  @Test
  void testOnlyDvOutputsDanglingDv() throws Exception {
    Table table = createPartitionedTable();
    DeleteFileInfo dvFile = dvFileInfo(table, DV_FILE_PATH, DATA_FILE_PATH);

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = testHarness()) {
      harness.open();

      harness.processElement1(dvFile, EVENT_TIME);
      assertThat(harness.extractOutputValues()).isEmpty();

      harness.processBothWatermarks(WATERMARK);

      List<DeleteFile> deleteFiles = harness.extractOutputValues();
      assertThat(deleteFiles).hasSize(1);
      assertDeleteFile(deleteFiles.get(0), dvFile);
    }
  }

  @Test
  void testDvWithExistingDataFileDoesNotOutput() throws Exception {
    Table table = createPartitionedTable();
    DeleteFileInfo dvFile = dvFileInfo(table, DV_FILE_PATH, DATA_FILE_PATH);

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = testHarness()) {
      harness.open();

      harness.processElement1(dvFile, EVENT_TIME);
      harness.processElement2(DATA_FILE_PATH, EVENT_TIME);
      assertThat(harness.extractOutputValues()).isEmpty();

      harness.processBothWatermarks(WATERMARK);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testDataFileFirstDoesNotOutput() throws Exception {
    Table table = createPartitionedTable();
    DeleteFileInfo dvFile = dvFileInfo(table, DV_FILE_PATH, DATA_FILE_PATH);

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = testHarness()) {
      harness.open();

      harness.processElement2(DATA_FILE_PATH, EVENT_TIME);
      harness.processElement1(dvFile, EVENT_TIME);
      assertThat(harness.extractOutputValues()).isEmpty();

      harness.processBothWatermarks(WATERMARK);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testDifferentDataFileDoesNotSuppressDanglingDv() throws Exception {
    Table table = createPartitionedTable();
    DeleteFileInfo dvFile = dvFileInfo(table, DV_FILE_PATH, DATA_FILE_PATH);

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = testHarness()) {
      harness.open();

      harness.processElement1(dvFile, EVENT_TIME);
      harness.processElement2(OTHER_DATA_FILE_PATH, EVENT_TIME);
      assertThat(harness.extractOutputValues()).isEmpty();

      harness.processBothWatermarks(WATERMARK);

      List<DeleteFile> deleteFiles = harness.extractOutputValues();
      assertThat(deleteFiles).hasSize(1);
      assertDeleteFile(deleteFiles.get(0), dvFile);
    }
  }

  @Test
  void testMultipleDvsForMissingDataFileAreOutput() throws Exception {
    Table table = createPartitionedTable();
    DeleteFileInfo firstDvFile = dvFileInfo(table, DV_FILE_PATH, DATA_FILE_PATH);
    DeleteFileInfo secondDvFile = dvFileInfo(table, OTHER_DV_FILE_PATH, DATA_FILE_PATH);

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = testHarness()) {
      harness.open();

      harness.processElement1(firstDvFile, EVENT_TIME);
      harness.processElement1(secondDvFile, EVENT_TIME);
      assertThat(harness.extractOutputValues()).isEmpty();

      harness.processBothWatermarks(WATERMARK);

      assertThat(harness.extractOutputValues())
          .extracting(deleteFile -> deleteFile.location())
          .containsExactlyInAnyOrder(DV_FILE_PATH, OTHER_DV_FILE_PATH);
    }
  }

  @Test
  void testRestoreState() throws Exception {
    Table table = createPartitionedTable();
    DeleteFileInfo dvFile = dvFileInfo(table, DV_FILE_PATH, DATA_FILE_PATH);
    OperatorSubtaskState state;

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = testHarness()) {
      harness.open();

      harness.processElement1(dvFile, EVENT_TIME);
      assertThat(harness.extractOutputValues()).isEmpty();
      state = harness.snapshot(1L, EVENT_TIME);
    }

    try (KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
        harness = uninitializedTestHarness()) {
      harness.initializeState(state);
      harness.open();

      harness.processBothWatermarks(WATERMARK);

      List<DeleteFile> deleteFiles = harness.extractOutputValues();
      assertThat(deleteFiles).hasSize(1);
      assertDeleteFile(deleteFiles.get(0), dvFile);
    }
  }

  private KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
      testHarness() throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
        new DanglingDvsDetector(tableLoader()),
        (KeySelector<DeleteFileInfo, String>)
            t -> RemoveDanglingDeletes.dvJoinKey(t.referencedDataFile()),
        (KeySelector<String, String>) RemoveDanglingDeletes::dvJoinKey,
        BasicTypeInfo.STRING_TYPE_INFO);
  }

  private KeyedTwoInputStreamOperatorTestHarness<String, DeleteFileInfo, String, DeleteFile>
      uninitializedTestHarness() throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(new DanglingDvsDetector(tableLoader())),
        (KeySelector<DeleteFileInfo, String>)
            t -> RemoveDanglingDeletes.dvJoinKey(t.referencedDataFile()),
        (KeySelector<String, String>) RemoveDanglingDeletes::dvJoinKey,
        BasicTypeInfo.STRING_TYPE_INFO);
  }

  private static DeleteFileInfo dvFileInfo(
      Table table, String filePath, String referencedDataFile) {
    return new DeleteFileInfo(
        GenericRowData.of(StringData.fromString(PARTITION)),
        table.spec().specId(),
        1L,
        FileContent.POSITION_DELETES.id(),
        null,
        filePath,
        FileFormat.PUFFIN.name(),
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        null,
        referencedDataFile,
        CONTENT_OFFSET,
        CONTENT_SIZE_IN_BYTES);
  }

  private static void assertDeleteFile(DeleteFile actual, DeleteFileInfo expected) {
    assertThat(actual.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(actual.location()).hasToString(expected.filePath());
    assertThat(actual.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.referencedDataFile()).isEqualTo(expected.referencedDataFile());
    assertThat(actual.contentOffset()).isEqualTo(expected.contentOffset());
    assertThat(actual.contentSizeInBytes()).isEqualTo(expected.contentSizeInBytes());
  }
}
