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

import java.io.Serializable;
import java.util.List;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;

public class DeleteFileInfo implements Serializable {
  private final RowData partition;
  private final Integer specId;
  private final Long sequenceNumber;
  private final Integer content;
  private final Integer status;
  private final String filePath;
  private final String fileFormat;
  private final Long recordCount;
  private final Long fileSizeInBytes;
  private final List<Integer> equalityFieldIds;
  private final String referencedDataFile;
  private final Long contentOffset;
  private final Long contentSizeInBytes;

  public DeleteFileInfo(RowData partition, Integer specId, Long sequenceNumber) {
    this(
        partition,
        specId,
        sequenceNumber,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public DeleteFileInfo(
      RowData partition,
      Integer specId,
      Long sequenceNumber,
      Integer content,
      Integer status,
      String filePath,
      String fileFormat,
      Long recordCount,
      Long fileSizeInBytes,
      List<Integer> equalityFieldIds,
      String referencedDataFile,
      Long contentOffset,
      Long contentSizeInBytes) {
    this.partition = partition;
    this.specId = specId;
    this.sequenceNumber = sequenceNumber;
    this.content = content;
    this.status = status;
    this.filePath = filePath;
    this.fileFormat = fileFormat;
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.equalityFieldIds = equalityFieldIds;
    this.referencedDataFile = referencedDataFile;
    this.contentOffset = contentOffset;
    this.contentSizeInBytes = contentSizeInBytes;
  }

  public Integer content() {
    return content;
  }

  public Integer status() {
    return status;
  }

  public RowData partition() {
    return partition;
  }

  public Integer specId() {
    return specId;
  }

  public Long sequenceNumber() {
    return sequenceNumber;
  }

  public String filePath() {
    return filePath;
  }

  public String fileFormat() {
    return fileFormat;
  }

  public Long recordCount() {
    return recordCount;
  }

  public Long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  public List<Integer> equalityFieldIds() {
    return equalityFieldIds;
  }

  public String referencedDataFile() {
    return referencedDataFile;
  }

  public Long contentOffset() {
    return contentOffset;
  }

  public Long contentSizeInBytes() {
    return contentSizeInBytes;
  }

  public DeleteFile toDeleteFile(Table table) {
    Preconditions.checkArgument(specId != null, "Delete file spec id is required");
    Preconditions.checkArgument(content != null, "Delete file content is required");
    Preconditions.checkArgument(filePath != null, "Delete file path is required");
    Preconditions.checkArgument(recordCount != null, "Delete file record count is required");
    Preconditions.checkArgument(fileSizeInBytes != null, "Delete file size is required");

    PartitionSpec spec = table.specs().get(specId);
    Preconditions.checkArgument(spec != null, "Unknown partition spec id: %s", specId);

    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(spec)
            .withPath(filePath)
            .withFileSizeInBytes(fileSizeInBytes)
            .withRecordCount(recordCount);

    if (fileFormat != null) {
      builder.withFormat(fileFormat);
    }

    StructLike partitionData = partitionData(spec);
    if (partitionData != null) {
      builder.withPartition(partitionData);
    }

    if (content == FileContent.POSITION_DELETES.id()) {
      builder.ofPositionDeletes();
    } else if (content == FileContent.EQUALITY_DELETES.id()) {
      Preconditions.checkArgument(
          equalityFieldIds != null && !equalityFieldIds.isEmpty(),
          "Equality delete file requires equality field ids");
      builder.ofEqualityDeletes(equalityFieldIds.stream().mapToInt(Integer::intValue).toArray());
    } else {
      throw new IllegalArgumentException("Unsupported delete file content: " + content);
    }

    if (referencedDataFile != null) {
      builder.withReferencedDataFile(referencedDataFile);
    }

    if (contentOffset != null) {
      builder.withContentOffset(contentOffset);
    }

    if (contentSizeInBytes != null) {
      builder.withContentSizeInBytes(contentSizeInBytes);
    }

    return builder.build();
  }

  private StructLike partitionData(PartitionSpec spec) {
    if (partition == null || !spec.isPartitioned()) {
      return null;
    }

    RowType partitionRowType = FlinkSchemaUtil.convert(new Schema(spec.partitionType().fields()));
    return new RowDataWrapper(partitionRowType, spec.partitionType()).wrap(partition);
  }
}
