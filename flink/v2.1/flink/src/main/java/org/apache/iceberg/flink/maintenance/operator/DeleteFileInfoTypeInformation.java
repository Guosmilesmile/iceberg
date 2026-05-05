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

import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
public class DeleteFileInfoTypeInformation extends TypeInformation<DeleteFileInfo> {
  private final RowType partitionRowType;

  public static DeleteFileInfoTypeInformation of(Table table) {
    Preconditions.checkNotNull(table, "Table is null");
    return new DeleteFileInfoTypeInformation(
        FlinkSchemaUtil.convert(new Schema(Partitioning.partitionType(table).fields())));
  }

  public DeleteFileInfoTypeInformation(RowType partitionRowType) {
    this.partitionRowType =
        Preconditions.checkNotNull(partitionRowType, "Partition row type is null");
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<DeleteFileInfo> getTypeClass() {
    return DeleteFileInfo.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<DeleteFileInfo> createSerializer(SerializerConfig serializerConfig) {
    return new DeleteFileInfoSerializer(partitionRowType);
  }

  @Override
  public String toString() {
    return getClass().getName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && getClass() == o.getClass()) {
      DeleteFileInfoTypeInformation that = (DeleteFileInfoTypeInformation) o;
      return Objects.equals(partitionRowType, that.partitionRowType);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(partitionRowType);
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof DeleteFileInfoTypeInformation;
  }
}
