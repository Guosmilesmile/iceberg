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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.api.Test;

public class TestPartitionTableChecker extends OperatorTestBase {

  @Test
  public void testUnPartitionTable() throws Exception {
    createTable();
    try (OneInputStreamOperatorTestHarness<Trigger, Trigger> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new PartitionTableChecker(tableLoader()))) {

      testHarness.open();
      testHarness.processElement(
          Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(0);
    }
  }

  @Test
  public void testPartitionTable() throws Exception {
    createPartitionedTable();
    try (OneInputStreamOperatorTestHarness<Trigger, Trigger> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new PartitionTableChecker(tableLoader()))) {

      testHarness.open();
      testHarness.processElement(
          Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(1);
    }
  }
}
