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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkipOnError
        extends CoProcessFunction<String, Exception, String> {
    private static final Logger LOG = LoggerFactory.getLogger(SkipOnError.class);
    private transient ListState<String> files;
    private transient ValueState<Boolean> hasError;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.files =
                getRuntimeContext()
                        .getListState(new ListStateDescriptor<>("blockOnErrorFiles", String.class));
        this.hasError =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("blockOnErrorHasError", Types.BOOLEAN));
    }

    @Override
    public void processElement1(String s, CoProcessFunction<String, Exception, String>.Context context, Collector<String> collector) throws Exception {
        context.timerService().registerEventTimeTimer(context.timestamp());
        files.add(s);
    }

    @Override
    public void processElement2(Exception e, CoProcessFunction<String, Exception, String>.Context context, Collector<String> collector) throws Exception {
        context.timerService().registerEventTimeTimer(context.timestamp());
        hasError.update(true);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        try {
            if (hasError.value() == null) {
                files.get().forEach(out::collect);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.info(
                            "Omitting result {} on failure at {}",
                            Lists.newArrayList(files.get().iterator()),
                            ctx.timestamp());
                } else {
                    LOG.info("Omitting result on failure at {}", ctx.timerService());
                }
            }
        } finally {
            files.clear();
            hasError.clear();
        }

        super.onTimer(timestamp, ctx, out);
    }
}
