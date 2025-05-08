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
package org.apache.iceberg.flink.maintenance.operator;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.reader.MetaDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads the records from the metadata table splits. */
public class TableReader extends ProcessFunction<IcebergSourceSplit, RowData>
    implements ResultTypeQueryable<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(TableReader.class);

  private final String name;
  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final RowType rowType;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final TypeSerializer<?>[] fieldSerializers;
  private final RowData.FieldGetter[] fieldGetters;
  private transient Table table;

  private transient MetaDataReaderFunction rowDataReaderFunction;
  private transient Counter errorCounter;

  public TableReader(String name, TableLoader tableLoader, Schema projectedSchema) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table should no be null");
    Preconditions.checkNotNull(projectedSchema, "The projected schema should no be null");

    tableLoader.open();
    this.table = MetadataTableUtils.createMetadataTableInstance(tableLoader.loadTable(), MetadataTableType.ALL_FILES);
    this.name = name;
    this.tableSchema = table.schema();
    this.projectedSchema = projectedSchema;
    this.rowType = FlinkSchemaUtil.convert(projectedSchema);
    this.io = table.io();
    this.encryption = table.encryption();
    this.fieldSerializers = FlinkSchemaUtil.createFieldSerializers(rowType);
    this.fieldGetters = FlinkSchemaUtil.createFieldGetters(rowType);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    /*this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);*/

    this.rowDataReaderFunction =
        new MetaDataReaderFunction(
            new Configuration(), tableSchema, projectedSchema, io, encryption);
  }

  @Override
  public void processElement(IcebergSourceSplit split, Context ctx, Collector<RowData> out)
      throws Exception {
    try (DataIterator<RowData> iterator = rowDataReaderFunction.createDataIterator(split)) {
      iterator.forEachRemaining(
          r ->
              out.collect(
                  RowDataUtil.clone(
                      r,
                      new GenericRowData(rowType.getFieldCount()),
                      rowType,
                      fieldSerializers,
                      fieldGetters)));
    } catch (Exception e) {
      LOG.info("Exception processing split {} at {}", split, ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return InternalTypeInfo.of(rowType);
  }
}
