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
package org.apache.iceberg.parquet;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.InternalFileEncryptor;
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

  @FunctionalInterface
  public interface ValueCopyFunction<T> {
    /**
     * Creates a copy of the value for buffering.
     *
     * @param value the value to copy
     * @return a copy of the value
     */
    T copy(T value);
  }

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
  private final ValueCopyFunction<T> valueCopyFunction;
  private final int bufferSize;

  private final List<BufferedData<T>> bufferedData;
  private ParquetValueWriter<T> actualWriter;
  private boolean writerInitialized = false;

  public DeferredParquetWriter(
      S schema,
      MessageType parquetType,
      SchemaInferenceFunction<T, S> schemaInferenceFunction,
      WriterFactory<T, S> writerFactory,
      ValueCopyFunction<T> valueCopyFunction,
      int bufferSize) {
    this.schema = schema;
    this.parquetType = parquetType;
    this.schemaInferenceFunction =
        Preconditions.checkNotNull(
            schemaInferenceFunction, "Schema inference function cannot be null");
    this.writerFactory = Preconditions.checkNotNull(writerFactory, "Writer factory cannot be null");
    this.valueCopyFunction =
        Preconditions.checkNotNull(valueCopyFunction, "Value copy function cannot be null");
    this.bufferSize = bufferSize > 0 ? bufferSize : 10;
    this.bufferedData = Lists.newArrayList();
  }

  @Override
  public void write(int repetitionLevel, T value) {
    if (!writerInitialized) {
      bufferedData.add(new BufferedData<>(repetitionLevel, valueCopyFunction.copy(value)));

      if (bufferedData.size() >= bufferSize) {
        writerInitialized = true;
      }
    } else {
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
    return !writerInitialized;
  }

  @Override
  public InitializationResult initialize(
      ParquetProperties props,
      CompressionCodecFactory.BytesInputCompressor compressor,
      int rowGroupOrdinal,
      int columnIndexTruncateLength,
      InternalFileEncryptor fileEncryptor) {

    if (bufferedData.isEmpty()) {
      throw new IllegalStateException("No buffered data available for schema inference");
    }

    List<T> values = Lists.newArrayListWithCapacity(bufferedData.size());
    for (BufferedData<T> data : bufferedData) {
      values.add(data.getValue());
    }

    MessageType inferredSchema = schemaInferenceFunction.inferSchema(values, schema, parquetType);

    actualWriter = writerFactory.createWriter(schema, inferredSchema);

    ColumnChunkPageWriteStore pageStore =
        new ColumnChunkPageWriteStore(
            compressor,
            inferredSchema,
            props.getAllocator(),
            columnIndexTruncateLength,
            ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
            fileEncryptor,
            rowGroupOrdinal);

    ColumnWriteStore columnStore = props.newColumnWriteStore(inferredSchema, pageStore, pageStore);

    actualWriter.setColumnStore(columnStore);

    for (BufferedData<T> data : bufferedData) {
      actualWriter.write(data.getRepetitionLevel(), data.getValue());
      columnStore.endRecord();
    }

    bufferedData.clear();
    writerInitialized = true;

    return new InitializationResult(inferredSchema, pageStore, columnStore);
  }
}
