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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregates error results. Input 1 is only used to make sure there is a result on watermark. Input
 * 2 expects a {@link Exception} which caused the failure. The operator emits a {@link RunResponse}
 * with the overall result.
 */
public class ErrorAggregator
        extends KeyedCoProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Exception>, RunResponse> {
    public static final OutputTag<Exception> ERROR_STREAM =
            new OutputTag<>("error-stream", TypeInformation.of(Exception.class));

    private static final Logger LOG = LoggerFactory.getLogger(ErrorAggregator.class);

    private final int id;
    private transient ValueState<Long> startTime;
    private transient ListState<Exception> exceptions;

    public ErrorAggregator(int id) {
        this.id = id;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.startTime =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("errorAggregatorStartTime", Types.LONG));
        this.exceptions =
                getRuntimeContext()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "errorAggregatorExceptions", TypeInformation.of(Exception.class)));
    }

    @Override
    public void processElement1(Tuple2<Long, Long> value, Context ctx, Collector<RunResponse> out)
            throws IOException {
        startTime.update(value.f1);
        ctx.timerService().registerEventTimeTimer(value.f0);
    }

    @Override
    public void processElement2(
            Tuple2<Long, Exception> value, Context ctx, Collector<RunResponse> out) throws Exception {
        Preconditions.checkNotNull(value.f1, "Exception could not be `null`.");
        ctx.timerService().registerEventTimeTimer(value.f0);
        exceptions.add(value.f1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RunResponse> out)
            throws Exception {
        Iterator<Exception> collected = exceptions.get().iterator();
        long length = ctx.timerService().currentProcessingTime() - startTime.value();
        RunResponse response;
        if (collected.hasNext()) {
            response = new RunResponse(timestamp, id, length, false, Lists.newArrayList(collected));
        } else {
            response = new RunResponse(timestamp, id, length, true, Collections.emptyList());
        }

        out.collect(response);

        LOG.info("Aggregated result is {}", response);
        exceptions.clear();
        startTime.clear();
    }
}
