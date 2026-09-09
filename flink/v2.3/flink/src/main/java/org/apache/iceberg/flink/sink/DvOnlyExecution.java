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
package org.apache.iceberg.flink.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Guards the execution model the DV-only write path is built on.
 *
 * <p>Deletes are resolved against the primary key index, and positions turned into deletion
 * vectors, when an operator handles a checkpoint barrier or its input ends. That relies on every
 * record sent before that point having been processed, and none sent after it. Only aligned
 * exactly-once checkpoints give that guarantee: unaligned checkpoints overtake in-flight records,
 * and at-least-once checkpoints do not align barriers at all, which would let a record of the next
 * checkpoint be applied early or one of the current checkpoint late.
 *
 * <p>The index also has to live in keyed state across the whole job, which batch execution does not
 * provide: it keeps the state of one key only until the sorted input moves on to the next.
 */
class DvOnlyExecution {

  /**
   * Checkpoint id reported for what the operators emit when their input ends. {@link
   * IcebergWriteAggregator} collects files regardless of the checkpoint they are reported for.
   */
  static final long END_OF_INPUT = Long.MAX_VALUE;

  private DvOnlyExecution() {}

  /** Validates the configuration of the job the sink is added to. */
  static void checkSupported(StreamExecutionEnvironment env) {
    Preconditions.checkState(
        env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE) != RuntimeExecutionMode.BATCH,
        "%s requires streaming execution, but %s is %s",
        FlinkWriteOptions.DV_ONLY_ENABLE.key(),
        ExecutionOptions.RUNTIME_MODE.key(),
        RuntimeExecutionMode.BATCH);
    // Without checkpointing there are no barriers to align: everything is resolved when the input
    // ends. The consistency mode is reported as at-least-once then, regardless of the setting.
    if (env.getCheckpointConfig().isCheckpointingEnabled()) {
      checkAligned(
          env.getCheckpointConfig().getCheckpointingConsistencyMode(),
          env.getCheckpointConfig().isUnalignedCheckpointsEnabled());
    }
  }

  /**
   * Validates the checkpoint configuration the job actually runs with. The configured mode is read
   * directly, since {@link CheckpointingOptions#getCheckpointingMode} reports at-least-once
   * whenever the configuration does not enable checkpointing itself.
   */
  static void checkAligned(Configuration jobConfiguration) {
    checkAligned(
        jobConfiguration.get(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE),
        jobConfiguration.get(CheckpointingOptions.ENABLE_UNALIGNED));
  }

  /**
   * Validates that a keyed operator does not run in batch execution, which a job in {@link
   * RuntimeExecutionMode#AUTOMATIC} mode picks when all its sources are bounded. Batch execution is
   * recognized by the sorted keyed inputs it requires.
   */
  static void checkStreaming(StreamConfig operatorConfig, ClassLoader classLoader) {
    for (StreamConfig.InputConfig input : operatorConfig.getInputs(classLoader)) {
      Preconditions.checkState(
          !(input instanceof StreamConfig.NetworkInputConfig networkInput)
              || networkInput.getInputRequirement() != StreamConfig.InputRequirement.SORTED,
          "%s requires streaming execution, but the job runs in batch execution",
          FlinkWriteOptions.DV_ONLY_ENABLE.key());
    }
  }

  private static void checkAligned(CheckpointingMode mode, boolean unaligned) {
    Preconditions.checkState(
        mode == CheckpointingMode.EXACTLY_ONCE,
        "%s requires %s checkpoints, but the job uses %s: barriers are not aligned, so deletes "
            + "could be resolved against the wrong checkpoint",
        FlinkWriteOptions.DV_ONLY_ENABLE.key(),
        CheckpointingMode.EXACTLY_ONCE,
        mode);
    Preconditions.checkState(
        !unaligned,
        "%s cannot be used with unaligned checkpoints (%s=true): records overtaken by a barrier "
            + "would be resolved against the wrong checkpoint",
        FlinkWriteOptions.DV_ONLY_ENABLE.key(),
        CheckpointingOptions.ENABLE_UNALIGNED.key());
  }
}
