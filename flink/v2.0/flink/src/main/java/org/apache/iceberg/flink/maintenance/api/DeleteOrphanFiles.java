/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.maintenance.api;

import java.time.Duration;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.maintenance.operator.AntiJoin;
import org.apache.iceberg.flink.maintenance.operator.DeleteFilesProcessor;
import org.apache.iceberg.flink.maintenance.operator.ErrorAggregator;
import org.apache.iceberg.flink.maintenance.operator.ListFileSystemFiles;
import org.apache.iceberg.flink.maintenance.operator.ListMetadataFilesProcess;
import org.apache.iceberg.flink.maintenance.operator.SkipOnError;
import org.apache.iceberg.flink.maintenance.operator.TablePlanner;
import org.apache.iceberg.flink.maintenance.operator.TableReader;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the delete orphan files data stream. Which runs a single iteration of the task for every
 * input event.
 *
 * <p>The input is a {@link DataStream} with {@link SerializableTable} events and every event should
 * be immediately followed by a {@link org.apache.flink.streaming.api.watermark.Watermark} with the
 * same timestamp as the event.
 *
 * <p>The output is a {@link DataStream} with the result of the run (success or failure) followed by
 * the {@link org.apache.flink.streaming.api.watermark.Watermark}.
 */
public class DeleteOrphanFiles {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteOrphanFiles.class);
    private static final Schema FILE_PATH_SCHEMA = new Schema(DataFile.FILE_PATH);
    private static final ScanContext FILE_PATH_SCAN_CONTEXT =
            ScanContext.builder().streaming(true).project(FILE_PATH_SCHEMA).build();
    private static final int DELETE_BATCH_SIZE_DEFAULT = 1000;


    static final String PLANNER_TASK_NAME = "Planner";
    static final String READER_TASK_NAME = "Reader";
    static final String FILESYSTEM_FILES_TASK_NAME = "Filesystem files";
    static final String METADATA_FILES_TASK_NAME = "List metadata files";
    static final String DELETE_FILES_TASK_NAME = "Delete file";

    private DeleteOrphanFiles() {
        // Do not instantiate directly
    }

    public static class Builder extends MaintenanceTaskBuilder<DeleteOrphanFiles.Builder> {

        private String location = null;
        private Duration minAge = Duration.ofDays(1);
        private int planningWorkerPoolSize = 10;
        private int deleteBatchSize = DELETE_BATCH_SIZE_DEFAULT;

        /**
         * The location to start the recursive listing the candidate files for removal. By default, the
         * {@link Table#location()} is used.
         *
         * @param newLocation the task will scan
         * @return for chained calls
         */
        public Builder location(String newLocation) {
            this.location = newLocation;
            return this;
        }

        /**
         * The files newer than this age will not be removed.
         *
         * @param newMinAge of the files to be removed
         * @return for chained calls
         */
        public Builder minAge(Duration newMinAge) {
            this.minAge = newMinAge;
            return this;
        }

        /**
         * The worker pool size used for planning the scan of the {@link MetadataTableType#ALL_FILES}
         * table. This scan is used for determining the files used by the table.
         *
         * @param newPlanningWorkerPoolSize for scanning
         * @return for chained calls
         */
        public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
            this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
            return this;
        }

        /**
         * The number of file to delete .
         *
         * @param newDeleteBatchSize number of batch file
         * @return for chained calls
         */
        public Builder deleteBatchSize(int newDeleteBatchSize) {
            this.deleteBatchSize = newDeleteBatchSize;
            return this;
        }


        @Override
        DataStream<TaskResult> append(DataStream<Trigger> trigger) {

            // Collect all data files
            SingleOutputStreamOperator<IcebergSourceSplit> splits =
                    trigger
                            .process(
                                    new TablePlanner("allTablePlanner", tableLoader(), FILE_PATH_SCAN_CONTEXT, planningWorkerPoolSize))
                            .name(PLANNER_TASK_NAME)
                            .uid("" + uidSuffix())
                            .slotSharingGroup(slotSharingGroup())
                            .forceNonParallel();

            // Read the records
            SingleOutputStreamOperator<RowData> rowData =
                    splits
                            .rebalance()
                            .process(new TableReader("name()", tableLoader(), FILE_PATH_SCHEMA))
                            .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(FILE_PATH_SCHEMA)))
                            .name(READER_TASK_NAME)
                            .uid("uidPrefix()" + "-all-data-files-reader")
                            .slotSharingGroup(slotSharingGroup())
                            .setParallelism(parallelism());


            // Extract the file name from the records
            DataStream<String> tableDataFiles =
                    rowData
                            .flatMap((FlatMapFunction<RowData, String>) (value, collector) -> {
                                if (value != null && value.getString(0) != null) {
                                    collector.collect(value.getString(0).toString());
                                }
                            })
                            .returns(Types.STRING)
                            .name("File name")
                            .uid("uidPrefix()" + "-file-name-extractor")
                            .slotSharingGroup(slotSharingGroup())
                            .setParallelism(parallelism());


            // Collect all meta data files
            SingleOutputStreamOperator<String> tableMetadataFiles =
                    trigger
                            .process(new ListMetadataFilesProcess("name()", tableLoader()))
                            .name(METADATA_FILES_TASK_NAME)
                            .uid("uidPrefix()" + "-metadata-file-list")
                            .slotSharingGroup(slotSharingGroup())
                            .forceNonParallel();


            // List the all file system files
            SingleOutputStreamOperator<String> allFsFiles = trigger
                    .process(
                            new ListFileSystemFiles(
                                    "name()",
                                    tableLoader(),
                                    location,
                                    minAge.toMillis()
                            ))
                    .name(FILESYSTEM_FILES_TASK_NAME)
                    .uid("uidPrefix()" + "-list-filesystem-files")
                    .slotSharingGroup(slotSharingGroup())
                    .forceNonParallel();

            SingleOutputStreamOperator<String> filesToDelete = tableMetadataFiles.union(tableDataFiles)
                    .keyBy(s -> s)
                    .connect(allFsFiles)
                    .process(new AntiJoin())
                    .slotSharingGroup(slotSharingGroup())
                    .name("Filter files")
                    .uid("uidPrefix()" + "-filter-unreachable")
                    .setParallelism(parallelism());


            // Aggregate the errors for event time
            DataStream<Exception> errorStream = tableMetadataFiles.getSideOutput(ErrorAggregator.ERROR_STREAM)
                    .union(
                            splits.getSideOutput(ErrorAggregator.ERROR_STREAM),
                            rowData.getSideOutput(ErrorAggregator.ERROR_STREAM),
                            allFsFiles.getSideOutput(ErrorAggregator.ERROR_STREAM));

            // Stop deleting the files if there is an error
            SingleOutputStreamOperator<String> filesOrSkip =
                    filesToDelete
                            .connect(errorStream)
                            .process(new SkipOnError())
                            .name("Skip on error")
                            .uid("uidPrefix()" + "-do-skip-on-error")
                            .slotSharingGroup(slotSharingGroup())
                            .setParallelism(parallelism());


            // delete the files
            filesOrSkip
                    .rebalance()
                    .transform(
                            operatorName(DELETE_FILES_TASK_NAME),
                            TypeInformation.of(Void.class),
                            new DeleteFilesProcessor(
                                    tableLoader().loadTable(), taskName(), index(), deleteBatchSize))
                    .uid(DELETE_FILES_TASK_NAME + uidSuffix())
                    .slotSharingGroup(slotSharingGroup())
                    .setParallelism(parallelism());


            return trigger
                    .connect(errorStream)
                    .transform(
                            operatorName("AGGREGATOR_TASK_NAME"),
                            TypeInformation.of(TaskResult.class),
                            new TaskResultAggregator(tableName(), taskName(), index()))
                    .uid("AGGREGATOR_TASK_NAME" + uidSuffix())
                    .slotSharingGroup(slotSharingGroup())
                    .forceNonParallel();

        }


    }
}
