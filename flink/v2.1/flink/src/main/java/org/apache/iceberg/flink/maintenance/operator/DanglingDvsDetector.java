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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

@Internal
public class DanglingDvsDetector
    extends KeyedCoProcessFunction<String, DeleteFileInfo, String, DeleteFile> {

  private final TableLoader tableLoader;

  private transient ListState<DeleteFileInfo> dvFiles;
  private transient ValueState<Boolean> dataFileExists;
  private transient Table table;

  public DanglingDvsDetector(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.dvFiles =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "danglingDvCandidates", DeleteFileInfoTypeInformation.of(table)));
    this.dataFileExists =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("danglingDvDataFileExists", Types.BOOLEAN));
  }

  @Override
  public void processElement1(DeleteFileInfo value, Context ctx, Collector<DeleteFile> out)
      throws Exception {
    dvFiles.add(value);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void processElement2(String value, Context ctx, Collector<DeleteFile> out)
      throws Exception {
    dataFileExists.update(Boolean.TRUE);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<DeleteFile> out)
      throws Exception {
    if (!Boolean.TRUE.equals(dataFileExists.value())) {
      for (DeleteFileInfo dvFile : dvFiles.get()) {
        out.collect(dvFile.toDeleteFile(table));
      }
    }

    dvFiles.clear();
    dataFileExists.clear();
  }
}
