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

import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeleteFileInfoSerializer extends OperatorTestBase {

  private DeleteFileInfoSerializer serializer;
  private RowType partitionRowType;
  private Table table;

  @BeforeEach
  void before() {
    this.table = createPartitionedTable();
    this.partitionRowType =
        FlinkSchemaUtil.convert(new Schema(Partitioning.partitionType(table).fields()));
    this.serializer = new DeleteFileInfoSerializer(partitionRowType);
  }

  @Test
  void testSerialize() throws Exception {
    DeleteFileInfo deleteFileInfo =
        new DeleteFileInfo(
            GenericRowData.of(StringData.fromString("p1")),
            table.spec().specId(),
            10L,
            FileContent.EQUALITY_DELETES.id(),
            1,
            "s3://bucket/table/delete.parquet",
            "PARQUET",
            20L,
            200L,
            Arrays.asList(1, null, 3),
            "s3://bucket/table/data.parquet",
            30L,
            300L);

    DeleteFileInfo deserialized = serializeAndDeserialize(deleteFileInfo);
    assertDeleteFileInfoEquals(deserialized, deleteFileInfo);
  }

  @Test
  void testCopy() {
    DeleteFileInfo deleteFileInfo =
        new DeleteFileInfo(
            GenericRowData.of(StringData.fromString("p1")),
            table.spec().specId(),
            10L,
            FileContent.EQUALITY_DELETES.id(),
            1,
            "s3://bucket/table/delete.parquet",
            "PARQUET",
            20L,
            200L,
            Arrays.asList(1, 3),
            "s3://bucket/table/data.parquet",
            30L,
            300L);

    DeleteFileInfo copied = serializer.copy(deleteFileInfo);

    assertThat(copied).isNotSameAs(deleteFileInfo);
    assertThat(copied.partition()).isNotSameAs(deleteFileInfo.partition());
    assertThat(copied.equalityFieldIds()).isNotSameAs(deleteFileInfo.equalityFieldIds());
    assertDeleteFileInfoEquals(copied, deleteFileInfo);
  }

  @Test
  void testCopyFromInputView() throws Exception {
    DeleteFileInfo deleteFileInfo = deleteFileInfo(table, "p1", 10L);

    DataOutputSerializer originalOutput = new DataOutputSerializer(1024);
    serializer.serialize(deleteFileInfo, originalOutput);

    DataOutputSerializer copiedOutput = new DataOutputSerializer(1024);
    serializer.copy(new DataInputDeserializer(originalOutput.getCopyOfBuffer()), copiedOutput);

    DeleteFileInfo copied =
        serializer.deserialize(new DataInputDeserializer(copiedOutput.getCopyOfBuffer()));
    assertDeleteFileInfoEquals(copied, deleteFileInfo);
  }

  @Test
  void testDuplicate() {
    TypeSerializer<DeleteFileInfo> duplicate = serializer.duplicate();

    assertThat(duplicate).isNotSameAs(serializer);
    assertThat(duplicate).isEqualTo(serializer);
  }

  @Test
  void testRestoredSerializer() throws Exception {
    DeleteFileInfo deleteFileInfo = deleteFileInfo(table, "p1", 10L);
    TypeSerializerSnapshot<DeleteFileInfo> snapshot = roundTrip(serializer.snapshotConfiguration());
    TypeSerializer<DeleteFileInfo> restoredSerializer = snapshot.restoreSerializer();

    DataOutputSerializer output = new DataOutputSerializer(1024);
    serializer.serialize(deleteFileInfo, output);

    DeleteFileInfo deserialized =
        restoredSerializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    assertDeleteFileInfoEquals(deserialized, deleteFileInfo);
  }

  @Test
  void testSnapshotIsCompatibleWithSamePartitionRowType() throws Exception {
    DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot oldSnapshot =
        new DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot(partitionRowType);
    DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot newSnapshot =
        roundTrip(new DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot(partitionRowType));

    TypeSerializerSchemaCompatibility<DeleteFileInfo> resultCompatibility =
        newSnapshot.resolveSchemaCompatibility(oldSnapshot);

    assertThat(resultCompatibility.isCompatibleAsIs()).isTrue();
  }

  @Test
  void testSnapshotIsIncompatibleWithDifferentPartitionRowType() throws Exception {
    RowType newPartitionRowType =
        FlinkSchemaUtil.convert(
            new Schema(Types.NestedField.optional(1, "other_partition", Types.StringType.get())));
    DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot oldSnapshot =
        new DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot(partitionRowType);
    DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot newSnapshot =
        roundTrip(
            new DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot(newPartitionRowType));

    TypeSerializerSchemaCompatibility<DeleteFileInfo> resultCompatibility =
        newSnapshot.resolveSchemaCompatibility(oldSnapshot);

    assertThat(resultCompatibility.isIncompatible()).isTrue();
  }

  private DeleteFileInfo serializeAndDeserialize(DeleteFileInfo deleteFileInfo) throws IOException {
    DataOutputSerializer output = new DataOutputSerializer(1024);
    serializer.serialize(deleteFileInfo, output);
    return serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
  }

  private static DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot roundTrip(
      TypeSerializerSnapshot<DeleteFileInfo> original) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(1024);
    original.writeSnapshot(out);

    DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot restored =
        new DeleteFileInfoSerializer.DeleteFileInfoSerializerSnapshot();
    DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
    restored.readSnapshot(restored.getCurrentVersion(), in, original.getClass().getClassLoader());
    return restored;
  }

  private static void assertDeleteFileInfoEquals(DeleteFileInfo actual, DeleteFileInfo expected) {
    assertPartitionEquals(actual.partition(), expected.partition());
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.sequenceNumber()).isEqualTo(expected.sequenceNumber());
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.status()).isEqualTo(expected.status());
    assertThat(actual.filePath()).isEqualTo(expected.filePath());
    assertThat(actual.fileFormat()).isEqualTo(expected.fileFormat());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
    assertThat(actual.referencedDataFile()).isEqualTo(expected.referencedDataFile());
    assertThat(actual.contentOffset()).isEqualTo(expected.contentOffset());
    assertThat(actual.contentSizeInBytes()).isEqualTo(expected.contentSizeInBytes());
  }

  private static void assertPartitionEquals(RowData actual, RowData expected) {
    if (expected == null) {
      assertThat(actual).isNull();
    } else {
      assertThat(actual).isNotNull();
      assertThat(actual.getArity()).isEqualTo(expected.getArity());
      assertThat(actual.getString(0).toString()).isEqualTo(expected.getString(0).toString());
    }
  }

  private static DeleteFileInfo deleteFileInfo(Table table, String partition, long sequenceNumber) {
    return new DeleteFileInfo(
        partition == null ? null : GenericRowData.of(StringData.fromString(partition)),
        table.spec().specId(),
        sequenceNumber);
  }
}
