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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

public class DanglingDeletesDetector
    extends KeyedCoProcessFunction<
        DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo, DeleteFile> {

  private final TableLoader tableLoader;

  private transient ListState<DeleteFileInfo> deleteEntities;
  private transient ListState<DeleteFileInfo> minSequenceNumberInfo;
  private transient Table table;

  public DanglingDeletesDetector(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    tableLoader.open();
    this.table = tableLoader.loadTable();

    DeleteFileInfoTypeInformation deleteFileInfoTypeInfo = DeleteFileInfoTypeInformation.of(table);
    deleteEntities =
        getRuntimeContext()
            .getListState(new ListStateDescriptor<>("deleteEntities", deleteFileInfoTypeInfo));

    minSequenceNumberInfo =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>("minSequenceNumberInfo", deleteFileInfoTypeInfo));
  }

  @Override
  public void processElement1(
      DeleteFileInfo value,
      KeyedCoProcessFunction<DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo, DeleteFile>
              .Context
          ctx,
      Collector<DeleteFile> out)
      throws Exception {
    deleteEntities.add(value);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void processElement2(
      DeleteFileInfo value,
      KeyedCoProcessFunction<DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo, DeleteFile>
              .Context
          ctx,
      Collector<DeleteFile> out)
      throws Exception {
    minSequenceNumberInfo.add(value);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedCoProcessFunction<DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo, DeleteFile>
              .OnTimerContext
          ctx,
      Collector<DeleteFile> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);

    Long minDataSequenceNumber = null;
    for (DeleteFileInfo value : minSequenceNumberInfo.get()) {
      if (value != null
          && value.sequenceNumber() != null
          && (minDataSequenceNumber == null || value.sequenceNumber() < minDataSequenceNumber)) {
        minDataSequenceNumber = value.sequenceNumber();
      }
    }

    for (DeleteFileInfo deleteEntry : deleteEntities.get()) {
      if (shouldEmit(deleteEntry, minDataSequenceNumber)) {
        out.collect(deleteEntry.toDeleteFile(table));
      }
    }

    deleteEntities.clear();
    minSequenceNumberInfo.clear();
  }

  private boolean shouldEmit(DeleteFileInfo deleteEntry, Long minDataSequenceNumber) {
    if (deleteEntry == null) {
      return false;
    }

    if (minDataSequenceNumber == null) {
      return true;
    }

    if (deleteEntry.content() == null || deleteEntry.sequenceNumber() == null) {
      return false;
    }

    if (deleteEntry.content() == FileContent.POSITION_DELETES.id()) {
      return deleteEntry.sequenceNumber() < minDataSequenceNumber;
    }

    if (deleteEntry.content() == FileContent.EQUALITY_DELETES.id()) {
      return deleteEntry.sequenceNumber() <= minDataSequenceNumber;
    }

    return false;
  }
}
