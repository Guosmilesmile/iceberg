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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class DanglingDeletesCommitter extends AbstractStreamOperator<Trigger>
    implements OneInputStreamOperator<DeleteFile, Trigger> {
  private static final Logger LOG = LoggerFactory.getLogger(DanglingDeletesCommitter.class);
  private final TableLoader tableLoader;

  private transient Table table;
  private transient List<DeleteFile> deleteFiles;

  public DanglingDeletesCommitter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open() throws Exception {
    super.open();

    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.deleteFiles = Lists.newArrayList();
  }

  @Override
  public void processElement(StreamRecord<DeleteFile> streamRecord) {
    deleteFiles.add(streamRecord.getValue());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    RewriteFiles rewriteFiles = table.newRewrite();
    DeleteFileSet danglingDeletes = DeleteFileSet.create();
    danglingDeletes.addAll(deleteFiles);
    for (DeleteFile deleteFile : danglingDeletes) {
      LOG.debug("Removing dangling delete file {}", deleteFile.location());
      rewriteFiles.deleteFile(deleteFile);
    }

    if (!danglingDeletes.isEmpty()) {
      rewriteFiles.commit();
      deleteFiles.clear();
    }

    super.processWatermark(mark);
  }
}
