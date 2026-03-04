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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.RegistryBasedFileWriterFactory;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.flink.data.FlinkSchemaInferenceVisitor;
import org.apache.iceberg.flink.data.ParquetWithFlinkSchemaVisitor;
import org.apache.iceberg.io.BufferedFileAppender;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;

public class FlinkFileWriterFactory extends RegistryBasedFileWriterFactory<RowData, RowType>
    implements Serializable {
  private final FileFormat dataFileFormat;
  private final Map<String, String> writeProperties;
  private final Schema dataSchema;
  private final Table table;
  private final RowType dataFlinkType;
  private final SortOrder dataSortOrder;

  FlinkFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      RowType dataFlinkType,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      RowType equalityDeleteFlinkType,
      SortOrder equalityDeleteSortOrder,
      Map<String, String> writeProperties) {

    super(
        table,
        dataFileFormat,
        RowData.class,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        writeProperties,
        dataFlinkType == null ? FlinkSchemaUtil.convert(dataSchema) : dataFlinkType,
        equalityDeleteInputSchema(equalityDeleteFlinkType, equalityDeleteRowSchema));
    this.dataFileFormat = dataFileFormat;
    this.writeProperties = writeProperties;
    this.dataSchema = dataSchema;
    this.table = table;
    this.dataFlinkType = dataFlinkType;
    this.dataSortOrder = dataSortOrder;
  }

  @Override
  public DataWriter<RowData> newDataWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    if (!shouldUseVariantShredding()) {
      return super.newDataWriter(file, spec, partition);
    }

    int bufferSize =
        Integer.parseInt(
            writeProperties.getOrDefault(
                FlinkWriteOptions.VARIANT_INFERENCE_BUFFER_SIZE.key(),
                String.valueOf(FlinkWriteOptions.VARIANT_INFERENCE_BUFFER_SIZE.defaultValue())));

    Map<String, String> tableProperties = table != null ? table.properties() : ImmutableMap.of();
    MetricsConfig metricsConfig =
        table != null ? MetricsConfig.forTable(table) : MetricsConfig.getDefault();

    Function<List<RowData>, FileAppender<RowData>> appenderFactory =
        bufferedRows -> {
          Preconditions.checkNotNull(bufferedRows, "bufferedRows must not be null");
          MessageType originalSchema = ParquetSchemaUtil.convert(dataSchema, "table");

          MessageType shreddedSchema =
              (MessageType)
                  ParquetWithFlinkSchemaVisitor.visit(
                      dataFlinkType,
                      originalSchema,
                      new FlinkSchemaInferenceVisitor(bufferedRows, dataFlinkType));

          try {
            FileAppender<RowData> appender =
                Parquet.write(file)
                    .schema(dataSchema)
                    .withFileSchema(shreddedSchema)
                    .createWriterFunc(
                        msgType -> FlinkParquetWriters.buildWriter(dataFlinkType, msgType))
                    .setAll(tableProperties)
                    .setAll(writeProperties)
                    .metricsConfig(metricsConfig)
                    .overwrite()
                    .build();

            for (RowData row : bufferedRows) {
              appender.add(row);
            }

            return appender;
          } catch (IOException e) {
            throw new UncheckedIOException("Failed to create shredded variant writer", e);
          }
        };

    RowDataSerializer serializer = new RowDataSerializer(dataFlinkType);
    BufferedFileAppender<RowData> bufferedAppender =
        new BufferedFileAppender<>(bufferSize, appenderFactory, serializer::copy);

    return new DataWriter<>(
        bufferedAppender,
        dataFileFormat,
        file.encryptingOutputFile().location(),
        spec,
        partition,
        file.keyMetadata(),
        dataSortOrder);
  }

  private static RowType equalityDeleteInputSchema(RowType rowType, Schema rowSchema) {
    if (rowType != null) {
      return rowType;
    } else if (rowSchema != null) {
      return FlinkSchemaUtil.convert(rowSchema);
    } else {
      return null;
    }
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  public static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private RowType dataFlinkType;
    private SortOrder dataSortOrder;
    private FileFormat deleteFileFormat;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private RowType equalityDeleteFlinkType;
    private SortOrder equalityDeleteSortOrder;
    private Map<String, String> writerProperties = ImmutableMap.of();

    public Builder(Table table) {
      this.table = table;

      Map<String, String> properties = table.properties();

      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.fromString(dataFileFormatName);

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.fromString(deleteFileFormatName);
    }

    public Builder dataFileFormat(FileFormat newDataFileFormat) {
      this.dataFileFormat = newDataFileFormat;
      return this;
    }

    public Builder dataSchema(Schema newDataSchema) {
      this.dataSchema = newDataSchema;
      return this;
    }

    /**
     * Sets a Flink type for data.
     *
     * <p>If not set, the value is derived from the provided Iceberg schema.
     */
    public Builder dataFlinkType(RowType newDataFlinkType) {
      this.dataFlinkType = newDataFlinkType;
      return this;
    }

    public Builder dataSortOrder(SortOrder newDataSortOrder) {
      this.dataSortOrder = newDataSortOrder;
      return this;
    }

    public Builder deleteFileFormat(FileFormat newDeleteFileFormat) {
      this.deleteFileFormat = newDeleteFileFormat;
      return this;
    }

    public Builder equalityFieldIds(int[] newEqualityFieldIds) {
      this.equalityFieldIds = newEqualityFieldIds;
      return this;
    }

    public Builder equalityDeleteRowSchema(Schema newEqualityDeleteRowSchema) {
      this.equalityDeleteRowSchema = newEqualityDeleteRowSchema;
      return this;
    }

    /**
     * Sets a Flink type for equality deletes.
     *
     * <p>If not set, the value is derived from the provided Iceberg schema.
     */
    public Builder equalityDeleteFlinkType(RowType newEqualityDeleteFlinkType) {
      this.equalityDeleteFlinkType = newEqualityDeleteFlinkType;
      return this;
    }

    public Builder equalityDeleteSortOrder(SortOrder newEqualityDeleteSortOrder) {
      this.equalityDeleteSortOrder = newEqualityDeleteSortOrder;
      return this;
    }

    /** Sets default writer properties. */
    public Builder writerProperties(Map<String, String> newWriterProperties) {
      this.writerProperties = newWriterProperties;
      return this;
    }

    public FlinkFileWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(
          noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new FlinkFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          dataFlinkType,
          dataSortOrder,
          deleteFileFormat,
          equalityFieldIds,
          equalityDeleteRowSchema,
          equalityDeleteFlinkType,
          equalityDeleteSortOrder,
          writerProperties);
    }
  }

  private boolean shouldUseVariantShredding() {
    // Variant shredding is currently only supported for Parquet files
    if (dataFileFormat != FileFormat.PARQUET) {
      return false;
    }

    boolean shreddingEnabled =
        Boolean.parseBoolean(writeProperties.get(FlinkWriteOptions.SHRED_VARIANTS.key()));

    return shreddingEnabled
        && dataSchema != null
        && dataSchema.columns().stream()
            .anyMatch(field -> field.type() instanceof Types.VariantType);
  }
}
