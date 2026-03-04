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
package org.apache.iceberg.spark.source;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.iceberg.parquet.AbstractSchemaInferenceVisitor;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.unsafe.types.VariantVal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A visitor that infers variant shredding schemas by analyzing buffered rows of data. */
public class SparkSchemaInferenceVisitor extends ParquetWithSparkSchemaVisitor<Type> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkSchemaInferenceVisitor.class);

  private final AbstractSchemaInferenceVisitor<StructType, InternalRow> delegate;

  public SparkSchemaInferenceVisitor(List<InternalRow> bufferedRows, StructType sparkSchema) {
    this.delegate = new SparkSchemaInference(bufferedRows, sparkSchema);
  }

  @Override
  public Type message(StructType sStruct, MessageType message, List<Type> fields) {
    return delegate.buildMessage(message, fields);
  }

  @Override
  public Type struct(StructType sStruct, GroupType struct, List<Type> fields) {
    return delegate.buildStruct(struct, fields);
  }

  @Override
  public Type primitive(DataType sPrimitive, PrimitiveType primitive) {
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

  private static class SparkSchemaInference
      extends AbstractSchemaInferenceVisitor<StructType, InternalRow> {

    SparkSchemaInference(List<InternalRow> bufferedRows, StructType schema) {
      super(bufferedRows, schema);
    }

    @Override
    protected VariantShreddingAnalyzer<InternalRow> createAnalyzer() {
      return new VariantShreddingAnalyzer<>() {
        @Override
        protected List<VariantValue> extractVariantValues(
            List<InternalRow> bufferedRows, int variantFieldIndex) {
          List<VariantValue> values = new java.util.ArrayList<>();

          for (InternalRow row : bufferedRows) {
            if (!row.isNullAt(variantFieldIndex)) {
              VariantVal variantVal = row.getVariant(variantFieldIndex);
              if (variantVal != null) {
                VariantValue variantValue =
                    VariantValue.from(
                        VariantMetadata.from(
                            ByteBuffer.wrap(variantVal.getMetadata())
                                .order(ByteOrder.LITTLE_ENDIAN)),
                        ByteBuffer.wrap(variantVal.getValue()).order(ByteOrder.LITTLE_ENDIAN));

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
            for (int i = 0; i < schema.fields().length; i++) {
              if (schema.fields()[i].name().equals(fieldName)) {
                return i;
              }
            }

            return -1;
          });
    }
  }
}
