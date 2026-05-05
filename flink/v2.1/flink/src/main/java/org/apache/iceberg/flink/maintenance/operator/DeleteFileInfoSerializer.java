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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.util.StringUtils;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
class DeleteFileInfoSerializer extends TypeSerializer<DeleteFileInfo> {
  private final RowType partitionRowType;
  private final TypeSerializer<RowData> partitionSerializer;
  private final int version;

  DeleteFileInfoSerializer(RowType partitionRowType) {
    this(partitionRowType, DeleteFileInfoSerializerSnapshot.CURRENT_VERSION);
  }

  private DeleteFileInfoSerializer(RowType partitionRowType, int version) {
    this.partitionRowType =
        Preconditions.checkNotNull(partitionRowType, "Partition row type is null");
    this.partitionSerializer = new RowDataSerializer(partitionRowType);
    this.version = version;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<DeleteFileInfo> duplicate() {
    return new DeleteFileInfoSerializer(partitionRowType, version);
  }

  @Override
  public DeleteFileInfo createInstance() {
    return new DeleteFileInfo(null, null, null);
  }

  @Override
  public DeleteFileInfo copy(DeleteFileInfo from) {
    if (from == null) {
      return null;
    }

    return new DeleteFileInfo(
        copyPartition(from.partition()),
        from.specId(),
        from.sequenceNumber(),
        from.content(),
        from.status(),
        from.filePath(),
        from.fileFormat(),
        from.recordCount(),
        from.fileSizeInBytes(),
        copyIntegerList(from.equalityFieldIds()),
        from.referencedDataFile(),
        from.contentOffset(),
        from.contentSizeInBytes());
  }

  @Override
  public DeleteFileInfo copy(DeleteFileInfo from, DeleteFileInfo reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(DeleteFileInfo record, DataOutputView target) throws IOException {
    writeNullableRowData(record.partition(), target);
    writeNullableInteger(record.specId(), target);
    writeNullableLong(record.sequenceNumber(), target);
    writeNullableInteger(record.content(), target);
    writeNullableInteger(record.status(), target);
    writeNullableString(record.filePath(), target);
    writeNullableString(record.fileFormat(), target);
    writeNullableLong(record.recordCount(), target);
    writeNullableLong(record.fileSizeInBytes(), target);
    writeNullableIntegerList(record.equalityFieldIds(), target);
    writeNullableString(record.referencedDataFile(), target);
    writeNullableLong(record.contentOffset(), target);
    writeNullableLong(record.contentSizeInBytes(), target);
  }

  @Override
  public DeleteFileInfo deserialize(DataInputView source) throws IOException {
    return deserialize(createInstance(), source);
  }

  @Override
  public DeleteFileInfo deserialize(DeleteFileInfo reuse, DataInputView source) throws IOException {
    return new DeleteFileInfo(
        readNullableRowData(source),
        readNullableInteger(source),
        readNullableLong(source),
        readNullableInteger(source),
        readNullableInteger(source),
        readNullableString(source),
        readNullableString(source),
        readNullableLong(source),
        readNullableLong(source),
        readNullableIntegerList(source),
        readNullableString(source),
        readNullableLong(source),
        readNullableLong(source));
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    serialize(deserialize(source), target);
  }

  @Override
  public TypeSerializerSnapshot<DeleteFileInfo> snapshotConfiguration() {
    return new DeleteFileInfoSerializerSnapshot(partitionRowType);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DeleteFileInfoSerializer other)) {
      return false;
    }

    return version == other.version && Objects.equals(partitionRowType, other.partitionRowType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionRowType, version);
  }

  private RowData copyPartition(RowData partition) {
    return partition == null ? null : partitionSerializer.copy(partition);
  }

  private void writeNullableRowData(RowData value, DataOutputView out) throws IOException {
    out.writeBoolean(value == null);
    if (value != null) {
      partitionSerializer.serialize(value, out);
    }
  }

  private RowData readNullableRowData(DataInputView in) throws IOException {
    boolean isNull = in.readBoolean();
    return isNull ? null : partitionSerializer.deserialize(in);
  }

  private static void writeNullableInteger(Integer value, DataOutputView out) throws IOException {
    out.writeBoolean(value == null);
    if (value != null) {
      out.writeInt(value);
    }
  }

  private static Integer readNullableInteger(DataInputView in) throws IOException {
    boolean isNull = in.readBoolean();
    return isNull ? null : in.readInt();
  }

  private static void writeNullableLong(Long value, DataOutputView out) throws IOException {
    out.writeBoolean(value == null);
    if (value != null) {
      out.writeLong(value);
    }
  }

  private static Long readNullableLong(DataInputView in) throws IOException {
    boolean isNull = in.readBoolean();
    return isNull ? null : in.readLong();
  }

  private static void writeNullableString(String value, DataOutputView out) throws IOException {
    out.writeBoolean(value == null);
    if (value != null) {
      StringUtils.writeString(value, out);
    }
  }

  private static String readNullableString(DataInputView in) throws IOException {
    boolean isNull = in.readBoolean();
    return isNull ? null : StringUtils.readString(in);
  }

  private static void writeNullableIntegerList(List<Integer> value, DataOutputView out)
      throws IOException {
    out.writeBoolean(value == null);
    if (value != null) {
      out.writeInt(value.size());
      for (Integer item : value) {
        writeNullableInteger(item, out);
      }
    }
  }

  private static List<Integer> readNullableIntegerList(DataInputView in) throws IOException {
    boolean isNull = in.readBoolean();
    if (isNull) {
      return null;
    }

    int size = in.readInt();
    List<Integer> values = new ArrayList<>(size);
    for (int index = 0; index < size; index++) {
      values.add(readNullableInteger(in));
    }

    return values;
  }

  private static List<Integer> copyIntegerList(List<Integer> value) {
    return value == null ? null : new ArrayList<>(value);
  }

  public static class DeleteFileInfoSerializerSnapshot
      implements TypeSerializerSnapshot<DeleteFileInfo> {
    private static final int CURRENT_VERSION = 1;

    private RowType partitionRowType;
    private int serializerVersion = CURRENT_VERSION;

    /** Constructor for read instantiation. */
    @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
    public DeleteFileInfoSerializerSnapshot() {
      // this constructor is used when restoring from a checkpoint.
    }

    DeleteFileInfoSerializerSnapshot(RowType partitionRowType) {
      this.partitionRowType = partitionRowType;
    }

    @Override
    public int getCurrentVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
      Preconditions.checkState(partitionRowType != null, "Invalid partition row type: null");
      StringUtils.writeString(partitionRowType.asSerializableString(), out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
        throws IOException {
      if (readVersion == 1) {
        LogicalType logicalType =
            LogicalTypeParser.parse(StringUtils.readString(in), userCodeClassLoader);
        Preconditions.checkArgument(
            logicalType instanceof RowType, "Invalid partition row type: %s", logicalType);
        this.partitionRowType = (RowType) logicalType;
        this.serializerVersion = readVersion;
      } else {
        throw new IllegalArgumentException("Unknown read version: " + readVersion);
      }
    }

    @Override
    public TypeSerializerSchemaCompatibility<DeleteFileInfo> resolveSchemaCompatibility(
        TypeSerializerSnapshot<DeleteFileInfo> oldSerializerSnapshot) {
      if (!(oldSerializerSnapshot instanceof DeleteFileInfoSerializerSnapshot oldSnapshot)) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      if (!Objects.equals(partitionRowType, oldSnapshot.partitionRowType)) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      if (oldSnapshot.serializerVersion == CURRENT_VERSION) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      }

      return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
    }

    @Override
    public TypeSerializer<DeleteFileInfo> restoreSerializer() {
      Preconditions.checkState(partitionRowType != null, "Invalid partition row type: null");
      return new DeleteFileInfoSerializer(partitionRowType, serializerVersion);
    }
  }
}
