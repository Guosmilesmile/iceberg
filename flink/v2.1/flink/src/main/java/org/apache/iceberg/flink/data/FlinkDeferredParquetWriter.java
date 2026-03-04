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

import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.parquet.DeferredParquetWriter;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;

public class FlinkDeferredParquetWriter extends DeferredParquetWriter<RowData, LogicalType> {

  public FlinkDeferredParquetWriter(
      LogicalType flinkSchema,
      MessageType parquetType,
      SchemaInferenceFunction<RowData, LogicalType> schemaInferenceFunction,
      int bufferSize) {
    super(
        flinkSchema,
        parquetType,
        schemaInferenceFunction,
        FlinkParquetWriters::buildWriter,
        value -> value,
        bufferSize);
  }

  public static FlinkDeferredParquetWriter forVariantShredding(
      LogicalType flinkSchema, MessageType parquetType, Map<String, String> properties) {
    int bufferSize =
        Integer.parseInt(
            properties.getOrDefault(
                FlinkWriteOptions.VARIANT_INFERENCE_BUFFER_SIZE.key(),
                String.valueOf(FlinkWriteOptions.VARIANT_INFERENCE_BUFFER_SIZE.defaultValue())));
    return new FlinkDeferredParquetWriter(
        flinkSchema,
        parquetType,
        (bufferedData, schema, originalParquetType) ->
            (MessageType)
                ParquetWithFlinkSchemaVisitor.visit(
                    schema,
                    originalParquetType,
                    new FlinkSchemaInferenceVisitor(bufferedData, schema)),
        bufferSize);
  }

  public static boolean shouldUseVariantShredding(Map<String, String> properties, Schema schema) {
    boolean shreddingEnabled =
        Boolean.parseBoolean(
            properties.getOrDefault(
                FlinkWriteOptions.SHRED_VARIANTS.key(),
                String.valueOf(FlinkWriteOptions.SHRED_VARIANTS.defaultValue())));

    boolean hasVariantFields =
        schema.columns().stream().anyMatch(field -> field.type() instanceof Types.VariantType);

    return shreddingEnabled && hasVariantFields;
  }
}
