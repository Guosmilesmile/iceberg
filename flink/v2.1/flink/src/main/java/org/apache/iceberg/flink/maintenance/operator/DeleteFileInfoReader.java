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

@Internal
public class DeleteFileInfoReader extends TableReader<DeleteFileInfo> {
  private final int partitionFieldCount;

  public DeleteFileInfoReader(
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
    if (rowData == null) {
      return;
    }

    Integer specId = rowData.isNullAt(0) ? null : rowData.getInt(0);
    Integer content = rowData.isNullAt(1) ? null : rowData.getInt(1);
    String filePath = rowData.isNullAt(2) ? null : rowData.getString(2).toString();
    String fileFormat = rowData.isNullAt(3) ? null : rowData.getString(3).toString();
    RowData partition = rowData.isNullAt(4) ? null : rowData.getRow(4, partitionFieldCount);
    Long recordCount = rowData.isNullAt(5) ? null : rowData.getLong(5);
    Long fileSizeInBytes = rowData.isNullAt(6) ? null : rowData.getLong(6);

    List<Integer> equalityFieldIds = null;
    if (!rowData.isNullAt(7)) {
      ArrayData equalityIdsArray = rowData.getArray(7);
      equalityFieldIds = Lists.newArrayListWithExpectedSize(equalityIdsArray.size());
      for (int pos = 0; pos < equalityIdsArray.size(); pos += 1) {
        equalityFieldIds.add(equalityIdsArray.getInt(pos));
      }
    }

    String referencedDataFile = rowData.isNullAt(8) ? null : rowData.getString(8).toString();
    Long contentOffset = rowData.isNullAt(9) ? null : rowData.getLong(9);
    Long contentSizeInBytes = rowData.isNullAt(10) ? null : rowData.getLong(10);

    out.collect(
        new DeleteFileInfo(
            partition,
            specId,
            null,
            content,
            null,
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
