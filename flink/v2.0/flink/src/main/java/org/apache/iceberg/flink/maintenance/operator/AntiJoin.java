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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class AntiJoin
        extends KeyedCoProcessFunction<
        String, String, String, String> {
    private transient ValueState<Boolean> foundInTable;
    private transient ValueState<Boolean> foundInFileSystem;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        foundInTable =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("antiJoinFoundInTable", Types.BOOLEAN));
        foundInFileSystem =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("antiJoinFoundInFileSystem", Types.BOOLEAN));
    }

    @Override
    public void processElement1(String s, KeyedCoProcessFunction<String, String, String, String>.Context context, Collector<String> collector) throws Exception {
        foundInTable.update(true);
        context.timerService().registerEventTimeTimer(context.timestamp());
    }

    @Override
    public void processElement2(String s, KeyedCoProcessFunction<String, String, String, String>.Context context, Collector<String> collector) throws Exception {
        foundInFileSystem.update(true);
        context.timerService().registerEventTimeTimer(context.timestamp());
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, String, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        if (foundInFileSystem.value() != null && foundInTable.value() == null) {
            out.collect(ctx.getCurrentKey());
        }

        foundInTable.clear();
        foundInFileSystem.clear();
    }
}
