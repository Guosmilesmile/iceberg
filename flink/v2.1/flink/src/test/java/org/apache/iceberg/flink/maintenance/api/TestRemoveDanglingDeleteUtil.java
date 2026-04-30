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
package org.apache.iceberg.flink.maintenance.api;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.ScanContext;

public class TestRemoveDanglingDeleteUtil {

  public static Schema projectedEntriesSchema(Table table) {
    return RemoveDanglingDeletes.projectedEntriesSchema(table);
  }

  public static ScanContext createScanContext(Schema projectedSchema) {
    return RemoveDanglingDeletes.createScanContext(projectedSchema);
  }

  public static Schema projectedDataFilesSchema() {
    return RemoveDanglingDeletes.projectedDataFilesSchema();
  }

  public static Schema projectedDeleteFilesSchema(Table table) {
    return RemoveDanglingDeletes.projectedDeleteFilesSchema(table);
  }
}
