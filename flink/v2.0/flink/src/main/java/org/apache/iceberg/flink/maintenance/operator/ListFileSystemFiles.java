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

import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recursively lists the files in the `location` directory. Hidden files, and files younger than the
 * `minAgeMs` are omitted in the result.
 */
public class ListFileSystemFiles extends ProcessFunction<Trigger, String> {
  private static final Logger LOG = LoggerFactory.getLogger(ListFileSystemFiles.class);

  private final String name;
  private final SupportsPrefixOperations io;
  private final Map<Integer, PartitionSpec> specs;
  private final String location;
  private final long minAgeMs;
  private transient Counter errorCounter;
  private TableLoader tableLoader;
  private transient Table table;

  public ListFileSystemFiles(
      String name,
      TableLoader tableLoader,
      String location,
      long minAgeMs) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(tableLoader, "TableLoad should no be null");

    tableLoader.open();
    this.table = tableLoader.loadTable();
    this.name = name;
    this.io = (SupportsPrefixOperations) table.io();
    this.location = location != null ? location : table.location();
    this.specs = table.specs();
    this.minAgeMs = minAgeMs;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
/*    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);*/
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<String> out) throws Exception {
    long olderThanTimestamp = trigger.timestamp() - minAgeMs;
    try {
      PathFilter filter = PartitionAwareHiddenPathFilter.forSpecs(specs);
      io.listPrefix(location)
          .forEach(
              file -> {
                if (filter.accept(new Path(file.location()))
                    && file.createdAtMillis() <= olderThanTimestamp) {
                  out.collect(file.location());
                }
              });
    } catch (Exception e) {
      LOG.info("Exception listing files for {} at {}", location, ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

}
