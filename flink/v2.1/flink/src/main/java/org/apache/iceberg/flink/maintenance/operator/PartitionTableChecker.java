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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class PartitionTableChecker extends ProcessFunction<Trigger, Trigger> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTableChecker.class);
  private final TableLoader tableLoader;

  private transient Table table;

  public PartitionTableChecker(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
  }

  @Override
  public void processElement(
      Trigger value, ProcessFunction<Trigger, Trigger>.Context ctx, Collector<Trigger> out)
      throws Exception {
    if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
      LOG.info("Table {} is unpartitioned, skip", table.name());
      return;
    }

    out.collect(value);
  }
}
