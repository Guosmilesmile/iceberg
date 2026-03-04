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
package org.apache.iceberg.flink.data;

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.iceberg.parquet.AbstractSchemaInferenceVisitor;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class FlinkSchemaInferenceVisitor extends ParquetWithFlinkSchemaVisitor<Type> {

  private final AbstractSchemaInferenceVisitor<LogicalType, RowData> delegate;

  public FlinkSchemaInferenceVisitor(List<RowData> bufferedRows, LogicalType flinkSchema) {
    this.delegate =
        new AbstractSchemaInferenceVisitor<>(bufferedRows, flinkSchema) {
          @Override
          protected VariantShreddingAnalyzer<RowData> createAnalyzer() {
            return new FlinkVariantShreddingAnalyzer();
          }

          @Override
          public int getFieldIndex(String[] path) {
            return getFieldIndexCommon(
                path,
                (schema, fieldName) -> {
                  RowType rowType = (RowType) schema;
                  for (int i = 0; i < rowType.getFields().size(); i++) {
                    if (rowType.getFields().get(i).getName().equals(fieldName)) {
                      return i;
                    }
                  }
                  return -1;
                });
          }
        };
  }

  @Override
  public Type message(RowType sStruct, MessageType message, List<Type> fields) {
    return delegate.buildMessage(message, fields);
  }

  @Override
  public Type struct(RowType sStruct, GroupType struct, List<Type> fields) {
    return delegate.buildStruct(struct, fields);
  }

  @Override
  public Type primitive(LogicalType sPrimitive, PrimitiveType primitive) {
    return primitive;
  }

  @Override
  public Type list(ArrayType sArray, GroupType array, Type element) {
    return delegate.buildList(array, element);
  }

  @Override
  public Type map(MapType sMap, GroupType map, Type key, Type value) {
    return delegate.buildMap(map, key, value);
  }

  @Override
  public Type variant(VariantType sVariant, GroupType variant) {
    int variantFieldIndex = delegate.getFieldIndex(currentPath());
    return delegate.buildVariant(variant, variantFieldIndex);
  }
}
