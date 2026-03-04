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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.iceberg.parquet.AbstractSchemaInferenceVisitor;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class FlinkSchemaInferenceVisitor extends ParquetWithFlinkSchemaVisitor<Type> {

  private final AbstractSchemaInferenceVisitor<RowType, RowData> delegate;

  public FlinkSchemaInferenceVisitor(List<RowData> bufferedRows, LogicalType flinkSchema) {
    RowType rowType = (RowType) flinkSchema;
    this.delegate = new FlinkSchemaInferenceDelegate(bufferedRows, rowType);
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
    return delegate.buildVariant(variant, delegate.getFieldIndex(currentPath()));
  }

  private static class FlinkSchemaInferenceDelegate
      extends AbstractSchemaInferenceVisitor<RowType, RowData> {

    FlinkSchemaInferenceDelegate(List<RowData> bufferedRows, RowType schema) {
      super(bufferedRows, schema);
    }

    @Override
    protected VariantShreddingAnalyzer<RowData> createAnalyzer() {
      return new VariantShreddingAnalyzer<>() {
        @Override
        protected List<VariantValue> extractVariantValues(
            List<RowData> bufferedRows, int variantFieldIndex) {
          List<VariantValue> values = new java.util.ArrayList<>();

          for (RowData row : bufferedRows) {
            if (!row.isNullAt(variantFieldIndex)) {
              BinaryVariant flinkVariant = (BinaryVariant) row.getVariant(variantFieldIndex);
              if (flinkVariant != null) {
                VariantValue variantValue =
                    VariantValue.from(
                        VariantMetadata.from(
                            ByteBuffer.wrap(flinkVariant.getMetadata())
                                .order(ByteOrder.LITTLE_ENDIAN)),
                        ByteBuffer.wrap(flinkVariant.getValue()).order(ByteOrder.LITTLE_ENDIAN));

                values.add(variantValue);
              }
            }
          }

          return values;
        }
      };
    }

    @Override
    public int getFieldIndex(String[] path) {
      return getFieldIndexCommon(
          path,
          (schema, fieldName) -> {
            for (int i = 0; i < schema.getFields().size(); i++) {
              if (schema.getFields().get(i).getName().equals(fieldName)) {
                return i;
              }
            }

            return -1;
          });
    }
  }
}
