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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.operator.DanglingDeletesCommitter;
import org.apache.iceberg.flink.maintenance.operator.DanglingDeletesDetector;
import org.apache.iceberg.flink.maintenance.operator.DanglingDvsDetector;
import org.apache.iceberg.flink.maintenance.operator.DataFilePathReader;
import org.apache.iceberg.flink.maintenance.operator.DeleteFileInfo;
import org.apache.iceberg.flink.maintenance.operator.DeleteFileInfoReader;
import org.apache.iceberg.flink.maintenance.operator.DeleteFileInfoTypeInformation;
import org.apache.iceberg.flink.maintenance.operator.DeleteFilePartitionKey;
import org.apache.iceberg.flink.maintenance.operator.IncompatibleChangeBlocker;
import org.apache.iceberg.flink.maintenance.operator.MetadataTablePlanner;
import org.apache.iceberg.flink.maintenance.operator.MinSequenceNumberByPartitionCal;
import org.apache.iceberg.flink.maintenance.operator.SequenceNumberPartitionInfoReader;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;

/** Reads projected rows from the ENTRIES metadata table for dangling delete detection. */
public class RemoveDanglingDeletes {

  private static final String SPEC_CHANGE_NAME = "Spec change blocker";
  private static final String PLANNER_TASK_NAME = "Entries Planner";
  private static final String READER_TASK_NAME = "Entries Reader";
  private static final String DELETE_FILES_PLANNER_TASK_NAME = "Delete Files Planner";
  private static final String DELETE_FILES_READER_TASK_NAME = "Delete Files Reader";
  private static final String DATA_FILES_PLANNER_TASK_NAME = "Data Files Planner";
  private static final String DATA_FILES_READER_TASK_NAME = "Data Files Reader";
  private static final String MIN_SEQUENCE_NUMBER_BY_PARTITION_TASK_NAME =
      "Min Sequence Number By Partition";
  private static final String DELETE_ENTRIES_FILTER_TASK_NAME = "Delete Entries Filter";
  private static final String DV_FILTER_TASK_NAME = "DV Filter";
  private static final String DANGLING_DELETES_DETECTOR_TASK_NAME = "Dangling Deletes Detector";
  private static final String DANGLING_DVS_DETECTOR_TASK_NAME = "Dangling DVs Detector";
  private static final String COMMIT_TASK_NAME = "Commit Dangling Deletes";
  private static final String AGGREGATOR_TASK_NAME = "Entries Aggregator";
  private static final String NULL_DV_KEY = "__NULL_DV_KEY__";

  @Internal
  public static final OutputTag<Exception> ERROR_STREAM =
      new OutputTag<>("error-stream", TypeInformation.of(Exception.class));

  private RemoveDanglingDeletes() {}

  /** Creates the builder for creating a stream which reads rows from the ENTRIES table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<Builder> {
    private int planningWorkerPoolSize = ThreadPools.WORKER_THREAD_POOL_SIZE;
    private boolean failOnSchemaChange;

    @Override
    String maintenanceTaskName() {
      return "RemoveDanglingDeletes";
    }

    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    /**
     * If there is a spec change on the table, then the partition info in task becomes invalid. If
     * failOnSpecChange is set to <code>true</code>, then the job will stop with {@link
     * org.apache.flink.runtime.execution.SuppressRestartsException} to prevent job restarts. If
     * failOnSpecChange is set to <code>false</code>, then the job will continue to run but the job
     * will stop working.
     *
     * @param newFailOnSpecChange to stop the job on table spec change
     * @return for chained calls
     */
    public Builder failOnSpecChange(boolean newFailOnSpecChange) {
      this.failOnSchemaChange = newFailOnSpecChange;
      return this;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      Preconditions.checkNotNull(tableLoader(), "TableLoader should not be null");

      if(!tableLoader().isOpen()){
        tableLoader().open();
      }

      Table table = tableLoader().loadTable();
      TypeInformation<DeleteFileInfo> deleteFileInfoTypeInfo =
          DeleteFileInfoTypeInformation.of(table);
      RowType deleteFilePartitionRowType = DeleteFilePartitionKey.partitionRowType(table);
      Schema entriesSchema = projectedEntriesSchema(table);
      Schema deleteFilesSchema = projectedDeleteFilesSchema(table);
      Schema dataFilesSchema = projectedDataFilesSchema();
      ScanContext entriesScanContext = createScanContext(entriesSchema);
      ScanContext deleteFilesScanContext = createScanContext(deleteFilesSchema);
      ScanContext dataFilesScanContext = createScanContext(dataFilesSchema);

      int partitionFieldCount = Partitioning.partitionType(table).fields().size();

      SingleOutputStreamOperator<Trigger> partitionedTrigger =
          trigger
              .transform(
                  operatorName(SPEC_CHANGE_NAME),
                  TypeInformation.of(Trigger.class),
                  new IncompatibleChangeBlocker(
                      taskName(), index(), tableLoader(), failOnSchemaChange))
              .name(operatorName(SPEC_CHANGE_NAME))
              .uid(SPEC_CHANGE_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // Part one, find dangling delete files from ENTRIES
      SingleOutputStreamOperator<MetadataTablePlanner.SplitInfo> entrySplits =
          partitionedTrigger
              .process(
                  new MetadataTablePlanner(
                      taskName(),
                      index(),
                      tableLoader(),
                      entriesScanContext,
                      MetadataTableType.ENTRIES,
                      planningWorkerPoolSize))
              .name(operatorName(PLANNER_TASK_NAME))
              .uid(PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<DeleteFileInfo> entriesDataStream =
          entrySplits
              .rebalance()
              .process(
                  new SequenceNumberPartitionInfoReader(
                      taskName(),
                      index(),
                      tableLoader(),
                      entriesSchema,
                      entriesScanContext,
                      MetadataTableType.ENTRIES,
                      partitionFieldCount))
              .returns(deleteFileInfoTypeInfo)
              .name(operatorName(READER_TASK_NAME))
              .uid(READER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<DeleteFileInfo> minSequenceNumberByPartition =
          entriesDataStream
              .filter(
                  value ->
                      value.status() != null
                          && value.content() != null
                          && value.content() == 0
                          && value.status() < 2)
              .keyBy(DeleteFilePartitionKey.selector(deleteFilePartitionRowType))
              .process(new MinSequenceNumberByPartitionCal(deleteFilePartitionRowType))
              .returns(deleteFileInfoTypeInfo)
              .name(operatorName(MIN_SEQUENCE_NUMBER_BY_PARTITION_TASK_NAME))
              .uid("min-sequence-number-by-partition" + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<DeleteFileInfo> deleteEntries =
          entriesDataStream
              .filter(
                  value ->
                      value.status() != null
                          && value.content() != null
                          && value.content() != 0
                          && value.status() < 2)
              .name(operatorName(DELETE_ENTRIES_FILTER_TASK_NAME))
              .uid("delete-entries-filter" + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<DeleteFile> danglingDeletes =
          deleteEntries
              .keyBy(DeleteFilePartitionKey.selector(deleteFilePartitionRowType))
              .connect(
                  minSequenceNumberByPartition.keyBy(
                      DeleteFilePartitionKey.selector(deleteFilePartitionRowType)))
              .process(new DanglingDeletesDetector(tableLoader()))
              .name(operatorName(DANGLING_DELETES_DETECTOR_TASK_NAME))
              .uid("dangling-deletes-detector" + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      // Part two, find dangling DVs from DELETE_FILES + DATA_FILES
      SingleOutputStreamOperator<MetadataTablePlanner.SplitInfo> deleteFileSplits =
          partitionedTrigger
              .process(
                  new MetadataTablePlanner(
                      taskName(),
                      index(),
                      tableLoader(),
                      deleteFilesScanContext,
                      MetadataTableType.DELETE_FILES,
                      planningWorkerPoolSize))
              .name(operatorName(DELETE_FILES_PLANNER_TASK_NAME))
              .uid(DELETE_FILES_PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<DeleteFileInfo> deleteFilesDataStream =
          deleteFileSplits
              .rebalance()
              .process(
                  new DeleteFileInfoReader(
                      taskName(),
                      index(),
                      tableLoader(),
                      deleteFilesSchema,
                      deleteFilesScanContext,
                      MetadataTableType.DELETE_FILES,
                      partitionFieldCount))
              .returns(deleteFileInfoTypeInfo)
              .name(operatorName(DELETE_FILES_READER_TASK_NAME))
              .uid(DELETE_FILES_READER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<DeleteFileInfo> dvs =
          deleteFilesDataStream
              .filter(
                  value ->
                      value.fileFormat() != null
                          && FileFormat.PUFFIN.name().equals(value.fileFormat()))
              .name(operatorName(DV_FILTER_TASK_NAME))
              .uid("dv-filter" + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<MetadataTablePlanner.SplitInfo> dataFileSplits =
          partitionedTrigger
              .process(
                  new MetadataTablePlanner(
                      taskName(),
                      index(),
                      tableLoader(),
                      dataFilesScanContext,
                      MetadataTableType.DATA_FILES,
                      planningWorkerPoolSize))
              .name(operatorName(DATA_FILES_PLANNER_TASK_NAME))
              .uid(DATA_FILES_PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<String> dataFilesDataStream =
          dataFileSplits
              .rebalance()
              .process(
                  new DataFilePathReader(
                      taskName(),
                      index(),
                      tableLoader(),
                      dataFilesSchema,
                      dataFilesScanContext,
                      MetadataTableType.DATA_FILES))
              .name(operatorName(DATA_FILES_READER_TASK_NAME))
              .uid(DATA_FILES_READER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<DeleteFile> danglingDvs =
          dvs.keyBy(value -> dvJoinKey(value.referencedDataFile()))
              .connect(dataFilesDataStream.keyBy(RemoveDanglingDeletes::dvJoinKey))
              .process(new DanglingDvsDetector(tableLoader()))
              .name(operatorName(DANGLING_DVS_DETECTOR_TASK_NAME))
              .uid("dangling-dvs-detector" + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      // Part three, commit dangling delete files
      SingleOutputStreamOperator<Trigger> committed =
          danglingDeletes
              .union(danglingDvs)
              .transform(
                  operatorName(COMMIT_TASK_NAME),
                  TypeInformation.of(Trigger.class),
                  new DanglingDeletesCommitter(tableLoader()))
              .uid(COMMIT_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      DataStream<Exception> errorStream =
          entrySplits
              .getSideOutput(ERROR_STREAM)
              .union(
                  deleteFileSplits.getSideOutput(ERROR_STREAM),
                  dataFileSplits.getSideOutput(ERROR_STREAM),
                  entriesDataStream.getSideOutput(ERROR_STREAM),
                  deleteFilesDataStream.getSideOutput(ERROR_STREAM),
                  dataFilesDataStream.getSideOutput(ERROR_STREAM));

      return trigger
          .union(committed)
          .connect(errorStream)
          .transform(
              operatorName(AGGREGATOR_TASK_NAME),
              TypeInformation.of(TaskResult.class),
              new TaskResultAggregator(tableName(), taskName(), index()))
          .uid(AGGREGATOR_TASK_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }
  }

  static Schema projectedEntriesSchema(Table table) {
    Types.StructType partitionType = Partitioning.partitionType(table);

    return new Schema(
        Types.NestedField.required(0, "status", Types.IntegerType.get()),
        Types.NestedField.optional(3, "sequence_number", Types.LongType.get()),
        Types.NestedField.required(
            2,
            "data_file",
            Types.StructType.of(
                DataFile.SPEC_ID,
                DataFile.CONTENT,
                DataFile.FILE_PATH,
                DataFile.FILE_FORMAT,
                Types.NestedField.required(
                    DataFile.PARTITION_ID,
                    DataFile.PARTITION_NAME,
                    partitionType,
                    DataFile.PARTITION_DOC),
                DataFile.RECORD_COUNT,
                DataFile.FILE_SIZE,
                DataFile.EQUALITY_IDS,
                DataFile.REFERENCED_DATA_FILE,
                DataFile.CONTENT_OFFSET,
                DataFile.CONTENT_SIZE)));
  }

  static Schema projectedDataFilesSchema() {
    return new Schema(DataFile.FILE_PATH);
  }

  static Schema projectedDeleteFilesSchema(Table table) {
    Types.StructType partitionType = Partitioning.partitionType(table);

    return new Schema(
        DataFile.SPEC_ID,
        DataFile.CONTENT,
        DataFile.FILE_PATH,
        DataFile.FILE_FORMAT,
        Types.NestedField.required(
            DataFile.PARTITION_ID, DataFile.PARTITION_NAME, partitionType, DataFile.PARTITION_DOC),
        DataFile.RECORD_COUNT,
        DataFile.FILE_SIZE,
        DataFile.EQUALITY_IDS,
        DataFile.REFERENCED_DATA_FILE,
        DataFile.CONTENT_OFFSET,
        DataFile.CONTENT_SIZE);
  }

  static ScanContext createScanContext(Schema projectedSchema) {
    return ScanContext.builder().streaming(true).project(projectedSchema).build();
  }

  private static String dvJoinKey(String referencedDataFile) {
    return referencedDataFile == null ? NULL_DV_KEY : referencedDataFile;
  }
}
