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

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.DeferredParquetWriter;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * A Parquet output writer that performs variant shredding with schema inference.
 *
 * <p>The writer works in two phases: 1. Schema inference phase: Buffers initial rows and analyzes
 * variant data to infer schemas 2. Writing phase: Creates the actual Parquet writer with inferred
 * schemas and writes all data
 */
public class SparkDeferredParquetWriter extends DeferredParquetWriter<InternalRow, StructType> {

  public SparkDeferredParquetWriter(
      StructType sparkSchema,
      MessageType parquetType,
      SchemaInferenceFunction<InternalRow, StructType> schemaInferenceFunction,
      int bufferSize) {
    super(
        sparkSchema,
        parquetType,
        schemaInferenceFunction,
        SparkParquetWriters::buildWriter,
        InternalRow::copy,
        bufferSize);
  }

  public static SparkDeferredParquetWriter forVariantShredding(
      StructType sparkSchema, MessageType parquetType, Map<String, String> properties) {
    int bufferSize =
        Integer.parseInt(
            properties.getOrDefault(
                SparkSQLProperties.VARIANT_INFERENCE_BUFFER_SIZE,
                String.valueOf(SparkSQLProperties.VARIANT_INFERENCE_BUFFER_SIZE_DEFAULT)));
    return new SparkDeferredParquetWriter(
        sparkSchema,
        parquetType,
        (bufferedData, schema, originalParquetType) ->
            (MessageType)
                ParquetWithSparkSchemaVisitor.visit(
                    schema,
                    originalParquetType,
                    new SparkSchemaInferenceVisitor(bufferedData, schema)),
        bufferSize);
  }

  public static boolean shouldUseVariantShredding(Map<String, String> properties, Schema schema) {
    boolean shreddingEnabled =
        properties.containsKey(SparkSQLProperties.SHRED_VARIANTS)
            && Boolean.parseBoolean(properties.get(SparkSQLProperties.SHRED_VARIANTS));

    boolean hasVariantFields =
        schema.columns().stream().anyMatch(field -> field.type() instanceof Types.VariantType);

    return shreddingEnabled && hasVariantFields;
  }
}
