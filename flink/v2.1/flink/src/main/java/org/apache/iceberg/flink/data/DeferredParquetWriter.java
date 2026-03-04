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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.TripleWriter;
import org.apache.iceberg.parquet.WriterLazyInitializable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.schema.MessageType;

public class DeferredParquetWriter<T, S> implements ParquetValueWriter<T>, WriterLazyInitializable {

  /** Function interface for schema inference from buffered data */
  @FunctionalInterface
  public interface SchemaInferenceFunction<T, S> {
    /**
     * Infers the Parquet schema from buffered data.
     *
     * @param bufferedData the list of buffered data
     * @param schema the original schema
     * @param parquetType the original Parquet type
     * @return the inferred Parquet schema
     */
    MessageType inferSchema(List<T> bufferedData, S schema, MessageType parquetType);
  }

  /** Function interface for creating the actual writer */
  @FunctionalInterface
  public interface WriterFactory<T, S> {
    /**
     * Creates a ParquetValueWriter for the given schema and Parquet type.
     *
     * @param schema the schema
     * @param parquetType the Parquet type
     * @return a new ParquetValueWriter
     */
    ParquetValueWriter<T> createWriter(S schema, MessageType parquetType);
  }

  /** Internal class to hold buffered data with repetition level */
  private static class BufferedData<T> {
    private final int repetitionLevel;
    private final T value;

    BufferedData(int repetitionLevel, T value) {
      this.repetitionLevel = repetitionLevel;
      this.value = value;
    }

    int getRepetitionLevel() {
      return repetitionLevel;
    }

    T getValue() {
      return value;
    }
  }

  private final S schema;
  private final MessageType parquetType;
  private final SchemaInferenceFunction<T, S> schemaInferenceFunction;
  private final WriterFactory<T, S> writerFactory;
  private final int bufferSize;

  private final List<BufferedData<T>> bufferedData;
  private ParquetValueWriter<T> actualWriter;
  private boolean writerInitialized = false;

  /**
   * Creates a new DeferredParquetWriter.
   *
   * @param schema the schema (e.g., Flink LogicalType)
   * @param parquetType the initial Parquet schema
   * @param schemaInferenceFunction function to infer schema from buffered data
   * @param writerFactory factory to create the actual writer
   * @param bufferSize number of records to buffer before auto-initialization (default: 100)
   */
  public DeferredParquetWriter(
      S schema,
      MessageType parquetType,
      SchemaInferenceFunction<T, S> schemaInferenceFunction,
      WriterFactory<T, S> writerFactory,
      int bufferSize) {
    this.schema = schema;
    this.parquetType = parquetType;
    this.schemaInferenceFunction =
        Preconditions.checkNotNull(
            schemaInferenceFunction, "Schema inference function cannot be null");
    this.writerFactory = Preconditions.checkNotNull(writerFactory, "Writer factory cannot be null");
    this.bufferSize = bufferSize > 0 ? bufferSize : 100;
    this.bufferedData = Lists.newArrayList();
  }

  @Override
  public void write(int repetitionLevel, T value) {
    if (!writerInitialized) {
      // Buffer the data
      bufferedData.add(new BufferedData<>(repetitionLevel, value));

      if (bufferedData.size() >= bufferSize) {
        writerInitialized = true;
      }
    } else {
      // Write directly to actual writer
      if (actualWriter == null) {
        throw new IllegalStateException(
            "Writer is marked as initialized but actual writer is null. "
                + "Call initialize() before writing more data.");
      }

      actualWriter.write(repetitionLevel, value);
    }
  }

  @Override
  public List<TripleWriter<?>> columns() {
    if (actualWriter != null) {
      return actualWriter.columns();
    }

    return Collections.emptyList();
  }

  @Override
  public void setColumnStore(ColumnWriteStore columnStore) {
    // Ignored for lazy initialization - will be set on actualWriter after initialization
    if (actualWriter != null) {
      actualWriter.setColumnStore(columnStore);
    }
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    if (actualWriter != null) {
      return actualWriter.metrics();
    }

    return Stream.empty();
  }

  @Override
  public boolean needsInitialization() {
//    return bufferedData.size() >= bufferSize && !writerInitialized;
    return !writerInitialized;
  }

  @Override
  public InitializationResult initialize(
      ParquetProperties props,
      CompressionCodecFactory.BytesInputCompressor compressor,
      int rowGroupOrdinal) {

    if (bufferedData.isEmpty()) {
      throw new IllegalStateException("No buffered data available for schema inference");
    }

    // Extract values from buffered data for schema inference
    List<T> values = Lists.newArrayListWithCapacity(bufferedData.size());
    for (BufferedData<T> data : bufferedData) {
      values.add(data.getValue());
    }

    // Perform schema inference
    MessageType inferredSchema = schemaInferenceFunction.inferSchema(values, schema, parquetType);

    // Create the actual writer with inferred schema
    actualWriter = writerFactory.createWriter(schema, inferredSchema);

    // Create write stores with the inferred schema
    ColumnChunkPageWriteStore pageStore =
        new ColumnChunkPageWriteStore(
            compressor,
            inferredSchema,
            props.getAllocator(),
            64,
            ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
            null,
            rowGroupOrdinal);

    ColumnWriteStore columnStore = props.newColumnWriteStore(inferredSchema, pageStore, pageStore);

    // Set column store to actual writer
    actualWriter.setColumnStore(columnStore);

    // Replay buffered data
    for (BufferedData<T> data : bufferedData) {
      actualWriter.write(data.getRepetitionLevel(), data.getValue());
      columnStore.endRecord();
    }

    // Clear buffer and mark as initialized
    bufferedData.clear();
    writerInitialized = true;

    return new InitializationResult(inferredSchema, pageStore, columnStore);
  }

  public static boolean shouldUseVariantShredding(Map<String, String> properties, Schema schema) {
    boolean shreddingEnabled =
        Boolean.parseBoolean(
            properties.getOrDefault("write.parquet.variant-shredding.enabled", "false"));

    boolean hasVariantFields =
        schema.columns().stream().anyMatch(field -> field.type() instanceof Types.VariantType);

    return shreddingEnabled && hasVariantFields;
  }
}
