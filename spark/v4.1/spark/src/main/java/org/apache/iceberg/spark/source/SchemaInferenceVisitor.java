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

import java.util.List;
import org.apache.iceberg.parquet.AbstractSchemaInferenceVisitor;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor;
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

/** A visitor that infers variant shredding schemas by analyzing buffered rows of data. */
class SchemaInferenceVisitor extends ParquetWithSparkSchemaVisitor<Type> {

  private final AbstractSchemaInferenceVisitor<StructType, InternalRow> delegate;

  SchemaInferenceVisitor(List<InternalRow> bufferedRows, StructType sparkSchema) {
    this.delegate =
        new AbstractSchemaInferenceVisitor<>(bufferedRows, sparkSchema) {
          @Override
          protected VariantShreddingAnalyzer<InternalRow> createAnalyzer() {
            return new SparkVariantShreddingAnalyzer();
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
        };
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
    int variantFieldIndex = delegate.getFieldIndex(currentPath());
    return delegate.buildVariant(variant, variantFieldIndex);
  }
}
