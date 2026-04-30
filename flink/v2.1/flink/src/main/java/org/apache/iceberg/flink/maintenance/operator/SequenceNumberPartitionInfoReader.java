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

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Reads projected rows from the ENTRIES metadata table. */
@Internal
public class SequenceNumberPartitionInfoReader extends TableReader<DeleteFileInfo> {
  private final int partitionFieldCount;

  public SequenceNumberPartitionInfoReader(
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      Schema projectedSchema,
      ScanContext scanContext,
      MetadataTableType metadataTableType,
      int partitionFieldCount) {
    super(taskName, taskIndex, tableLoader, projectedSchema, scanContext, metadataTableType);
    Preconditions.checkArgument(
        partitionFieldCount >= 0, "Partition field count should be non-negative");
    this.partitionFieldCount = partitionFieldCount;
  }

  @Override
  void extract(RowData rowData, Collector<DeleteFileInfo> out) {
    if (rowData == null || rowData.isNullAt(1)) {
      return;
    }

    Integer status = rowData.isNullAt(0) ? null : rowData.getInt(0);
    Long sequenceNumber = rowData.isNullAt(1) ? null : rowData.getLong(1);
    RowData dataFile = rowData.getRow(2, 11);
    Integer specId = dataFile.isNullAt(0) ? null : dataFile.getInt(0);
    Integer content = dataFile.isNullAt(1) ? null : dataFile.getInt(1);
    String filePath = dataFile.isNullAt(2) ? null : dataFile.getString(2).toString();
    String fileFormat = dataFile.isNullAt(3) ? null : dataFile.getString(3).toString();
    RowData partition = dataFile.isNullAt(4) ? null : dataFile.getRow(4, partitionFieldCount);
    Long recordCount = dataFile.isNullAt(5) ? null : dataFile.getLong(5);
    Long fileSizeInBytes = dataFile.isNullAt(6) ? null : dataFile.getLong(6);
    List<Integer> equalityFieldIds = null;
    if (!dataFile.isNullAt(7)) {
      ArrayData equalityIdsArray = dataFile.getArray(7);
      equalityFieldIds = Lists.newArrayListWithExpectedSize(equalityIdsArray.size());
      for (int pos = 0; pos < equalityIdsArray.size(); pos += 1) {
        equalityFieldIds.add(equalityIdsArray.getInt(pos));
      }
    }

    String referencedDataFile = dataFile.isNullAt(8) ? null : dataFile.getString(8).toString();
    Long contentOffset = dataFile.isNullAt(9) ? null : dataFile.getLong(9);
    Long contentSizeInBytes = dataFile.isNullAt(10) ? null : dataFile.getLong(10);
    out.collect(
        new DeleteFileInfo(
            partition,
            specId,
            sequenceNumber,
            content,
            status,
            filePath,
            fileFormat,
            recordCount,
            fileSizeInBytes,
            equalityFieldIds,
            referencedDataFile,
            contentOffset,
            contentSizeInBytes));
  }
}
