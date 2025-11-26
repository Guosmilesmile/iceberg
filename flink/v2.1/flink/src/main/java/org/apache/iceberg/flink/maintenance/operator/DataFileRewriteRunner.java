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

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner.PlannedGroup;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.parquet.ParquetFileMerger;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a rewrite for a single {@link PlannedGroup}. Reads the files with the standard {@link
 * FileScanTaskReader}, so the delete files are considered, and writes using the {@link
 * TaskWriterFactory}. The output is an {@link ExecutedGroup}.
 */
@Internal
public class DataFileRewriteRunner
    extends ProcessFunction<PlannedGroup, DataFileRewriteRunner.ExecutedGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewriteRunner.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final boolean openParquetMerge;

  private transient int subTaskId;
  private transient int attemptId;
  private transient Counter errorCounter;

  public DataFileRewriteRunner(
      String tableName, String taskName, int taskIndex, boolean openParquetMerge) {
    Preconditions.checkNotNull(tableName, "Table name should no be null");
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    this.tableName = tableName;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.openParquetMerge = openParquetMerge;
  }

  @Override
  public void open(OpenContext context) {
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);

    this.subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
  }

  @Override
  public void processElement(PlannedGroup value, Context ctx, Collector<ExecutedGroup> out)
      throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Rewriting files for group {} with files: {}",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          value.group().info(),
          value.group().rewrittenFiles());
    } else {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Rewriting files for group {} with {} number of files",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          value.group().info(),
          value.group().rewrittenFiles().size());
    }

    boolean useParquetRowGroupMerge = false;
    try {
      useParquetRowGroupMerge = canUseParquetMerge(value.group(), value.table());
    } catch (Exception ex) {
      LOG.warn(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Exception checking if Parquet merge can be used for group {}",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          value.group(),
          ex);
    }

    if (openParquetMerge && useParquetRowGroupMerge) {
      try {
        // schema is lazy init, so we need to get it here,or it will be exception in kryoSerializes
        for (FileScanTask fileScanTask : value.group().fileScanTasks()) {
          fileScanTask.schema();
        }

        Set<DataFile> resultFileSet = rewriteDataFilesUseParquetMerge(value.group(), value.table());
        value.group().setOutputFiles(resultFileSet);
        ExecutedGroup executedGroup =
            new ExecutedGroup(
                value.table().currentSnapshot().snapshotId(),
                value.groupsPerCommit(),
                value.group());
        out.collect(executedGroup);
      } catch (Exception ex) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX
                + "Exception creating compaction writer for group {}",
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp(),
            value.group(),
            ex);
        ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
        errorCounter.inc();
      }
    } else {
      boolean preserveRowId = TableUtil.supportsRowLineage(value.table());

      try (TaskWriter<RowData> writer = writerFor(value, preserveRowId)) {
        try (DataIterator<RowData> iterator = readerFor(value, preserveRowId)) {
          while (iterator.hasNext()) {
            writer.write(iterator.next());
          }

          Set<DataFile> dataFiles = Sets.newHashSet(writer.dataFiles());
          value.group().setOutputFiles(dataFiles);
          out.collect(
              new ExecutedGroup(
                  value.table().currentSnapshot().snapshotId(),
                  value.groupsPerCommit(),
                  value.group()));
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                DataFileRewritePlanner.MESSAGE_PREFIX + "Rewritten files {} from {} to {}",
                tableName,
                taskName,
                taskIndex,
                ctx.timestamp(),
                value.group().info(),
                value.group().rewrittenFiles(),
                value.group().addedFiles());
          } else {
            LOG.info(
                DataFileRewritePlanner.MESSAGE_PREFIX + "Rewritten {} files to {} files",
                tableName,
                taskName,
                taskIndex,
                ctx.timestamp(),
                value.group().rewrittenFiles().size(),
                value.group().addedFiles().size());
          }
        } catch (Exception ex) {
          LOG.info(
              DataFileRewritePlanner.MESSAGE_PREFIX + "Exception rewriting datafile group {}",
              tableName,
              taskName,
              taskIndex,
              ctx.timestamp(),
              value.group(),
              ex);
          ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
          errorCounter.inc();
          abort(writer, ctx.timestamp());
        }
      } catch (Exception ex) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX
                + "Exception creating compaction writer for group {}",
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp(),
            value.group(),
            ex);
        ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
        errorCounter.inc();
      }
    }
  }

  private Set<DataFile> rewriteDataFilesUseParquetMerge(RewriteFileGroup group, Table table)
      throws IOException {
    List<List<DataFile>> rewrittenFilesList =
        groupFilesBySizeWithConstraints(
            group.rewrittenFiles(), group.expectedOutputFiles(), group.maxOutputFileSize());

    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, taskIndex, attemptId)
            .format(FileFormat.PARQUET)
            .ioSupplier(table::io)
            .defaultSpec(table.spec())
            .build();

    Set<DataFile> resultFileSet = Sets.newHashSet();
    for (List<DataFile> dataFiles : rewrittenFilesList) {
      OutputFile outputFile =
          outputFileFactory.newOutputFile(group.info().partition()).encryptingOutputFile();
      List<InputFile> inputFiles =
          dataFiles.stream()
              .map(f -> table.io().newInputFile(f.location()))
              .collect(Collectors.toList());
      ParquetFileMerger.mergeFiles(inputFiles, outputFile, null);
      InputFile outputFileInputFile = outputFile.toInputFile();

      DataFile resultFile =
          org.apache.iceberg.DataFiles.builder(table.spec())
              .withPath(outputFile.location())
              .withFormat(FileFormat.PARQUET)
              .withPartition(group.info().partition())
              .withFileSizeInBytes(outputFileInputFile.getLength())
              .withMetrics(ParquetUtil.fileMetrics(outputFileInputFile, MetricsConfig.getDefault()))
              .build();

      resultFileSet.add(resultFile);
    }

    return resultFileSet;
  }

  private boolean canUseParquetMerge(RewriteFileGroup group, Table table) {

    boolean preserveRowId = TableUtil.supportsRowLineage(table);
    if (preserveRowId) {
      LOG.debug("Cannot use row-group merge for V3+ table");
      return false;
    }

    boolean fileSizeBiggerThanMaxSize =
        group.rewrittenFiles().stream()
            .anyMatch(file -> file.fileSizeInBytes() > group.maxOutputFileSize());
    if (fileSizeBiggerThanMaxSize) {
      LOG.debug("Cannot use row-group merge: file size is bigger than max size");
      return false;
    }

    boolean allParquet =
        group.rewrittenFiles().stream().allMatch(file -> file.format() == FileFormat.PARQUET);
    if (!allParquet) {
      LOG.debug("Cannot use row-group merge: not all files are Parquet format");
      return false;
    }

    if (table.sortOrder().isSorted()) {
      LOG.debug(
          "Cannot use row-group merge: table has a sort order ({}). "
              + "Row-group merging would not preserve the sort order.",
          table.sortOrder());
      return false;
    }

    boolean hasDeletes = group.fileScanTasks().stream().anyMatch(task -> !task.deletes().isEmpty());
    if (hasDeletes) {
      LOG.debug(
          "Cannot use row-group merge: files have delete files or delete vectors. "
              + "Row-group merging cannot apply deletes.");
      return false;
    }

    boolean allTheSamePartition =
        group.rewrittenFiles().stream().anyMatch(file -> file.specId() != table.spec().specId());
    if (allTheSamePartition) {
      LOG.debug(
          "Cannot use row-group merge: files are not in the same partition. "
              + "Row-group merging cannot be applied to not same partition files.");
      return false;
    }

    List<org.apache.iceberg.io.InputFile> inputFiles =
        group.rewrittenFiles().stream()
            .map(f -> table.io().newInputFile(f.location()))
            .collect(Collectors.toList());
    boolean canMerge = ParquetFileMerger.canMerge(inputFiles);
    if (!canMerge) {
      LOG.warn(
          "Cannot use row-group merge: schema validation failed for {} files. "
              + "Falling back to standard rewrite.",
          group.rewrittenFiles().size());
      return false;
    }

    return true;
  }

  private List<List<DataFile>> groupFilesBySizeWithConstraints(
      Set<DataFile> rewrittenFiles, int expectedOutputFiles, long maxOutputFileSize) {
    List<List<DataFile>> groups = Lists.newArrayList();

    List<DataFile> sortedFiles =
        rewrittenFiles.stream()
            .sorted(Comparator.comparingLong(DataFile::fileSizeInBytes).reversed())
            .collect(Collectors.toList());

    List<DataFile> currentGroup = Lists.newArrayList();
    long currentSize = 0;

    for (DataFile file : sortedFiles) {
      long fileSize = file.fileSizeInBytes();

      if (fileSize > maxOutputFileSize) {
        if (!currentGroup.isEmpty()) {
          groups.add(currentGroup);
          currentGroup = Lists.newArrayList();
          currentSize = 0;
        }

        List<DataFile> largeFileGroup = Lists.newArrayList(file);
        groups.add(largeFileGroup);
        continue;
      }

      if (groups.size() >= expectedOutputFiles) {
        currentGroup.add(file);
        continue;
      }

      if (currentSize + fileSize > maxOutputFileSize && !currentGroup.isEmpty()) {
        groups.add(currentGroup);
        currentGroup = Lists.newArrayList();
        currentSize = 0;

        if (groups.size() >= expectedOutputFiles) {
          currentGroup.add(file);
          continue;
        }
      }

      currentGroup.add(file);
      currentSize += fileSize;
    }

    if (!currentGroup.isEmpty()) {
      groups.add(currentGroup);
    }

    return groups;
  }

  private TaskWriter<RowData> writerFor(PlannedGroup value, boolean preserveRowId) {
    String formatString =
        PropertyUtil.propertyAsString(
            value.table().properties(),
            TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    Schema writeSchema =
        preserveRowId
            ? MetadataColumns.schemaWithRowLineage(value.table().schema())
            : value.table().schema();
    RowType flinkWriteType = FlinkSchemaUtil.convert(writeSchema);
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            value::table,
            flinkWriteType,
            value.group().inputSplitSize(),
            FileFormat.fromString(formatString),
            value.table().properties(),
            null,
            false,
            writeSchema,
            value.table().spec());
    factory.initialize(subTaskId, attemptId);
    return factory.create();
  }

  private DataIterator<RowData> readerFor(PlannedGroup value, boolean preserveRowId) {
    Schema projectedSchema =
        preserveRowId
            ? MetadataColumns.schemaWithRowLineage(value.table().schema())
            : value.table().schema();

    RowDataFileScanTaskReader reader =
        new RowDataFileScanTaskReader(
            value.table().schema(),
            projectedSchema,
            PropertyUtil.propertyAsString(value.table().properties(), DEFAULT_NAME_MAPPING, null),
            false,
            Collections.emptyList());
    return new DataIterator<>(
        reader,
        new BaseCombinedScanTask(value.group().fileScanTasks()),
        value.table().io(),
        value.table().encryption());
  }

  private void abort(TaskWriter<RowData> writer, long timestamp) {
    try {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Aborting rewrite for (subTaskId {}, attemptId {})",
          tableName,
          taskName,
          taskIndex,
          timestamp,
          subTaskId,
          attemptId);
      writer.abort();
    } catch (Exception e) {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Exception in abort",
          tableName,
          taskName,
          taskIndex,
          timestamp,
          e);
    }
  }

  public static class ExecutedGroup {
    private final long snapshotId;
    private final int groupsPerCommit;
    private final RewriteFileGroup group;

    @VisibleForTesting
    ExecutedGroup(long snapshotId, int groupsPerCommit, RewriteFileGroup group) {
      this.snapshotId = snapshotId;
      this.groupsPerCommit = groupsPerCommit;
      this.group = group;
    }

    long snapshotId() {
      return snapshotId;
    }

    int groupsPerCommit() {
      return groupsPerCommit;
    }

    RewriteFileGroup group() {
      return group;
    }
  }
}
