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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

@Internal
public class MinSequenceNumberByPartitionCal
    extends KeyedProcessFunction<Tuple2<RowData, Integer>, DeleteFileInfo, DeleteFileInfo> {

  private transient ValueState<Long> minSequenceNumber;

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    minSequenceNumber =
        getRuntimeContext().getState(new ValueStateDescriptor<>("minSequenceNumber", Types.LONG));
  }

  public MinSequenceNumberByPartitionCal() {}

  @Override
  public void processElement(
      DeleteFileInfo value,
      KeyedProcessFunction<Tuple2<RowData, Integer>, DeleteFileInfo, DeleteFileInfo>.Context ctx,
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
      KeyedProcessFunction<Tuple2<RowData, Integer>, DeleteFileInfo, DeleteFileInfo>.OnTimerContext
          ctx,
      Collector<DeleteFileInfo> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);
    out.collect(
        new DeleteFileInfo(
            ctx.getCurrentKey().f0, ctx.getCurrentKey().f1, minSequenceNumber.value()));
    minSequenceNumber.clear();
  }
}
