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
import java.io.Serializable;
import java.util.Base64;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
public class DeleteFilePartitionKey implements Serializable {
  private static final long serialVersionUID = 1L;

  private Integer specId;
  private String serializedPartition;

  public DeleteFilePartitionKey() {}

  DeleteFilePartitionKey(Integer specId, String serializedPartition) {
    this.specId = specId;
    this.serializedPartition = serializedPartition;
  }

  public static RowType partitionRowType(Table table) {
    Preconditions.checkNotNull(table, "Table is null");
    return FlinkSchemaUtil.convert(new Schema(Partitioning.partitionType(table).fields()));
  }

  public static KeySelector<DeleteFileInfo, DeleteFilePartitionKey> selector(
      RowType partitionRowType) {
    return new Selector(partitionRowType);
  }

  static DeleteFilePartitionKey of(
      DeleteFileInfo info, TypeSerializer<RowData> partitionSerializer, DataOutputSerializer out)
      throws IOException {
    return new DeleteFilePartitionKey(
        info.specId(), serializePartition(info.partition(), partitionSerializer, out));
  }

  public Integer specId() {
    return specId;
  }

  public Integer getSpecId() {
    return specId;
  }

  public void setSpecId(Integer specId) {
    this.specId = specId;
  }

  public String serializedPartition() {
    return serializedPartition;
  }

  public String getSerializedPartition() {
    return serializedPartition;
  }

  public void setSerializedPartition(String serializedPartition) {
    this.serializedPartition = serializedPartition;
  }

  public RowData partition(TypeSerializer<RowData> partitionSerializer) throws IOException {
    if (serializedPartition == null) {
      return null;
    }

    byte[] bytes = Base64.getDecoder().decode(serializedPartition);
    return partitionSerializer.deserialize(new DataInputDeserializer(bytes));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof DeleteFilePartitionKey that) {
      return Objects.equals(specId, that.specId)
          && Objects.equals(serializedPartition, that.serializedPartition);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(specId, serializedPartition);
  }

  private static String serializePartition(
      RowData partition, TypeSerializer<RowData> partitionSerializer, DataOutputSerializer out)
      throws IOException {
    if (partition == null) {
      return null;
    }

    out.clear();
    partitionSerializer.serialize(partition, out);
    String encoded = Base64.getEncoder().encodeToString(out.getCopyOfBuffer());
    out.clear();
    return encoded;
  }

  private static class Selector implements KeySelector<DeleteFileInfo, DeleteFilePartitionKey> {
    private static final long serialVersionUID = 1L;

    private final RowType partitionRowType;

    private transient TypeSerializer<RowData> partitionSerializer;
    private transient DataOutputSerializer out;

    private Selector(RowType partitionRowType) {
      this.partitionRowType =
          Preconditions.checkNotNull(partitionRowType, "Partition row type is null");
    }

    @Override
    public DeleteFilePartitionKey getKey(DeleteFileInfo value) throws Exception {
      return DeleteFilePartitionKey.of(value, partitionSerializer(), outputBuffer());
    }

    private TypeSerializer<RowData> partitionSerializer() {
      if (partitionSerializer == null) {
        this.partitionSerializer = new RowDataSerializer(partitionRowType);
      }

      return partitionSerializer;
    }

    private DataOutputSerializer outputBuffer() {
      if (out == null) {
        this.out = new DataOutputSerializer(128);
      }

      return out;
    }
  }
}
