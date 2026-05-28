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

import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

@Internal
public class MinSequenceNumberByPartitionCalculate
    extends KeyedProcessFunction<DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo> {

  private final RowType partitionRowType;

  private transient ValueState<Long> minSequenceNumber;
  private transient TypeSerializer<RowData> partitionSerializer;

  public MinSequenceNumberByPartitionCalculate(RowType partitionRowType) {
    this.partitionRowType =
        Preconditions.checkNotNull(partitionRowType, "Partition row type is null");
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    minSequenceNumber =
        getRuntimeContext().getState(new ValueStateDescriptor<>("minSequenceNumber", Types.LONG));
  }

  @Override
  public void processElement(
      DeleteFileInfo value,
      KeyedProcessFunction<DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>.Context ctx,
      Collector<DeleteFileInfo> out)
      throws Exception {
    Long currentMinSequenceNumber = minSequenceNumber.value();
    if (currentMinSequenceNumber == null || value.sequenceNumber() < currentMinSequenceNumber) {
      minSequenceNumber.update(value.sequenceNumber());
    }

    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<DeleteFilePartitionKey, DeleteFileInfo, DeleteFileInfo>.OnTimerContext
          ctx,
      Collector<DeleteFileInfo> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);
    DeleteFilePartitionKey currentKey = ctx.getCurrentKey();
    out.collect(
        new DeleteFileInfo(
            currentKey.partition(partitionSerializer()),
            currentKey.specId(),
            minSequenceNumber.value()));
    minSequenceNumber.clear();
  }

  private TypeSerializer<RowData> partitionSerializer() {
    if (partitionSerializer == null) {
      this.partitionSerializer = new RowDataSerializer(partitionRowType);
    }

    return partitionSerializer;
  }
}
