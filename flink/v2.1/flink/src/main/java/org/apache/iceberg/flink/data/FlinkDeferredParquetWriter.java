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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.parquet.schema.MessageType;

public class FlinkDeferredParquetWriter extends DeferredParquetWriter<RowData, LogicalType> {

  /**
   * Creates a new FlinkDeferredParquetWriter.
   *
   * @param flinkSchema the Flink logical type schema
   * @param parquetType the initial Parquet schema
   * @param schemaInferenceFunction function to infer schema from buffered RowData
   * @param bufferSize number of records to buffer before auto-initialization
   */
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
        bufferSize);
  }

  /**
   * Convenience factory method to create a FlinkDeferredParquetWriter with variant shredding
   * support.
   *
   * @param flinkSchema the Flink logical type schema
   * @param parquetType the initial Parquet schema
   * @param bufferSize number of records to buffer before auto-initialization
   * @return a new FlinkDeferredParquetWriter configured for variant shredding
   */
  public static FlinkDeferredParquetWriter forVariantShredding(
      LogicalType flinkSchema, MessageType parquetType, int bufferSize) {
    return new FlinkDeferredParquetWriter(
        flinkSchema,
        parquetType,
        (bufferedData, schema, originalParquetType) -> {
          // Use the existing schema inference logic from FlinkVariantShreddingWriter
          return (MessageType)
              ParquetWithFlinkSchemaVisitor.visit(
                  schema, originalParquetType, new SchemaInferenceVisitor(bufferedData, schema));
        },
        bufferSize);
  }

  /**
   * Convenience factory method to create a FlinkDeferredParquetWriter with variant shredding
   * support using default buffer size.
   *
   * @param flinkSchema the Flink logical type schema
   * @param parquetType the initial Parquet schema
   * @return a new FlinkDeferredParquetWriter configured for variant shredding
   */
  public static FlinkDeferredParquetWriter forVariantShredding(
      LogicalType flinkSchema, MessageType parquetType) {
    return forVariantShredding(flinkSchema, parquetType, 100);
  }
}
