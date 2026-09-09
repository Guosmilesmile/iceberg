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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Flink SinkV2 {@link Committer} interface to implement the Iceberg
 * commits. The implementation builds on the following assumptions:
 *
 * <ul>
 *   <li>There is a single {@link IcebergCommittable} for every checkpoint
 *   <li>There is no late checkpoint - if checkpoint 'x' has received in one call, then after a
 *       successful run only checkpoints &gt; x will arrive
 *   <li>There is no other writer which would generate another commit to the same branch with the
 *       same jobId-operatorId-checkpointId triplet
 * </ul>
 */
class IcebergCommitter implements Committer<IcebergCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitter.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];
  public static final WriteResult EMPTY_WRITE_RESULT =
      WriteResult.builder()
          .addDataFiles(Lists.newArrayList())
          .addDeleteFiles(Lists.newArrayList())
          .build();

  @VisibleForTesting
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  /**
   * How often the deletion vectors of one checkpoint are merged again after a concurrent commit
   * made the previous attempt invalid, before the job is failed.
   */
  private static final int MAX_DV_ONLY_COMMIT_ATTEMPTS = 10;

  private final String branch;
  private final Map<String, String> snapshotProperties;
  private final boolean replacePartitions;
  private IcebergFilesCommitterMetrics committerMetrics;
  private Table table;
  private final TableLoader tableLoader;
  private int maxContinuousEmptyCommits;
  private ExecutorService workerPool;
  private int continuousEmptyCheckpoints = 0;
  private final boolean tableMaintenanceEnabled;
  private final boolean dvOnlyMode;
  private DvOnlyDeletionVectorMerger deletionVectorMerger;
  private final int subtaskId;

  IcebergCommitter(
      TableLoader tableLoader,
      String branch,
      Map<String, String> snapshotProperties,
      boolean replacePartitions,
      int workerPoolSize,
      String sinkId,
      IcebergFilesCommitterMetrics committerMetrics,
      boolean tableMaintenanceEnabled,
      int subtaskId) {
    this(
        tableLoader,
        branch,
        snapshotProperties,
        replacePartitions,
        workerPoolSize,
        sinkId,
        committerMetrics,
        tableMaintenanceEnabled,
        false,
        ImmutableSet.of(),
        subtaskId);
  }

  IcebergCommitter(
      TableLoader tableLoader,
      String branch,
      Map<String, String> snapshotProperties,
      boolean replacePartitions,
      int workerPoolSize,
      String sinkId,
      IcebergFilesCommitterMetrics committerMetrics,
      boolean tableMaintenanceEnabled,
      boolean dvOnlyMode,
      Set<Integer> equalityFieldIds,
      int subtaskId) {
    this.branch = branch;
    this.snapshotProperties = snapshotProperties;
    this.replacePartitions = replacePartitions;
    this.committerMetrics = committerMetrics;
    this.tableLoader = tableLoader;
    this.tableMaintenanceEnabled = tableMaintenanceEnabled;
    this.dvOnlyMode = dvOnlyMode;
    this.subtaskId = subtaskId;

    // IcebergSink#addPreCommitTopology routes all committables to subtask 0 via a .global()
    // shuffle, so only subtask 0 needs to load the table and create the worker pool.
    if (subtaskId == 0) {
      if (!tableLoader.isOpen()) {
        tableLoader.open();
      }

      this.table = tableLoader.loadTable();
      this.maxContinuousEmptyCommits =
          PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
      Preconditions.checkArgument(
          maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
      this.workerPool =
          ThreadPools.newFixedThreadPool(
              "iceberg-committer-pool-" + table.name() + "-" + sinkId, workerPoolSize);
      if (dvOnlyMode) {
        this.deletionVectorMerger =
            new DvOnlyDeletionVectorMerger(
                table,
                branch,
                equalityFieldIds,
                OutputFileFactory.builderFor(table, subtaskId, 0L)
                    .format(FileFormat.PUFFIN)
                    .build());
      }
    }

    this.continuousEmptyCheckpoints = 0;
  }

  @Override
  public void commit(Collection<CommitRequest<IcebergCommittable>> commitRequests)
      throws IOException, InterruptedException {
    Preconditions.checkState(
        subtaskId == 0,
        "IcebergCommitter received %s commit request(s) on subtask %s, but only subtask 0 is expected to commit.",
        commitRequests.size(),
        subtaskId);

    if (commitRequests.isEmpty()) {
      return;
    }

    NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap = Maps.newTreeMap();
    for (CommitRequest<IcebergCommittable> request : commitRequests) {
      commitRequestMap.put(request.getCommittable().checkpointId(), request);
    }

    IcebergCommittable last = commitRequestMap.lastEntry().getValue().getCommittable();
    long maxCommittedCheckpointId =
        SinkUtil.getMaxCommittedCheckpointId(table, last.jobId(), last.operatorId(), branch);
    // Mark the already committed FilesCommittable(s) as finished
    commitRequestMap
        .headMap(maxCommittedCheckpointId, true)
        .values()
        .forEach(CommitRequest::signalAlreadyCommitted);
    NavigableMap<Long, CommitRequest<IcebergCommittable>> uncommitted =
        commitRequestMap.tailMap(maxCommittedCheckpointId, false);
    if (!uncommitted.isEmpty()) {
      commitPendingRequests(uncommitted, last.jobId(), last.operatorId());
    }
  }

  /**
   * Commits the data to the Iceberg table by reading the file data from the {@link
   * org.apache.iceberg.flink.sink.DeltaManifests} ordered by the checkpointId, and writing the new
   * snapshot to the Iceberg table. The {@link org.apache.iceberg.SnapshotSummary} will contain the
   * jobId, snapshotId, checkpointId so in case of job restart we can identify which changes are
   * committed, and which are still waiting for the commit.
   *
   * @param commitRequestMap The checkpointId to {@link CommitRequest} map of the changes to commit
   * @param newFlinkJobId The jobId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @param operatorId The operatorId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @throws IOException On commit failure
   */
  private void commitPendingRequests(
      NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap,
      String newFlinkJobId,
      String operatorId)
      throws IOException {
    long checkpointId = commitRequestMap.lastKey();
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    Map<Long, Long> baselineSnapshotIds = Maps.newHashMap();
    for (Map.Entry<Long, CommitRequest<IcebergCommittable>> e : commitRequestMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue().getCommittable().manifest())) {
        pendingResults.put(e.getKey(), EMPTY_WRITE_RESULT);
      } else {
        DeltaManifests deltaManifests =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                DeltaManifestsSerializer.INSTANCE, e.getValue().getCommittable().manifest());
        pendingResults.put(
            e.getKey(),
            FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
        baselineSnapshotIds.put(e.getKey(), deltaManifests.baselineSnapshotId());
        manifests.addAll(deltaManifests.manifests());
      }
    }

    CommitSummary summary = new CommitSummary(pendingResults);
    commitPendingResult(pendingResults, baselineSnapshotIds, summary, newFlinkJobId, operatorId);
    if (committerMetrics != null) {
      committerMetrics.updateCommitSummary(summary);
    }

    if (!tableMaintenanceEnabled) {
      FlinkManifestUtil.deleteCommittedManifests(table, manifests, newFlinkJobId, checkpointId);
    }
  }

  private void logCommitSummary(CommitSummary summary, String description) {
    LOG.info(
        "Preparing for commit: {} on table: {} branch: {} with summary: {}.",
        description,
        table,
        branch,
        summary);
  }

  private void commitPendingResult(
      NavigableMap<Long, WriteResult> pendingResults,
      Map<Long, Long> baselineSnapshotIds,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId) {
    long totalFiles = summary.dataFilesCount() + summary.deleteFilesCount();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(pendingResults, summary, newFlinkJobId, operatorId);
      } else {
        commitDeltaTxn(pendingResults, baselineSnapshotIds, summary, newFlinkJobId, operatorId);
      }
      continuousEmptyCheckpoints = 0;
    } else {
      long checkpointId = pendingResults.lastKey();
      LOG.info("Skip commit for checkpoint {} due to no data files or delete files.", checkpointId);
    }
  }

  private void replacePartitions(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId) {
    long checkpointId = pendingResults.lastKey();
    Preconditions.checkState(
        summary.deleteFilesCount() == 0, "Cannot overwrite partitions with delete files.");
    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(
          result.referencedDataFiles().length == 0, "Should have no referenced data files.");
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }
    String description = "dynamic partition overwrite";

    logCommitSummary(summary, description);
    commitOperation(dynamicOverwrite, description, newFlinkJobId, operatorId, checkpointId);
  }

  private void commitDeltaTxn(
      NavigableMap<Long, WriteResult> pendingResults,
      Map<Long, Long> baselineSnapshotIds,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId) {
    long checkpointId = pendingResults.lastKey();
    if (summary.deleteFilesCount() == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(
            result.referencedDataFiles().length == 0,
            "Should have no referenced data files for append.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }
      String description = "append";
      logCommitSummary(summary, description);
      // fail all commits as really its only one
      commitOperation(appendFiles, description, newFlinkJobId, operatorId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential
        // transaction txn1 and txn2, the equality-delete files of txn2 are required to be applied
        // to data files from txn1. Committing the merged one will lead to the incorrect delete
        // semantic.
        WriteResult result = e.getValue();
        if (dvOnlyMode) {
          commitDvOnlyDelta(
              result,
              baselineSnapshotIds.get(e.getKey()),
              summary,
              newFlinkJobId,
              operatorId,
              e.getKey());
          continue;
        }

        // Row delta validations are not needed for streaming changes that write equality deletes.
        // Equality deletes are applied to data in all previous sequence numbers, so retries may
        // push deletes further in the future, but do not affect correctness. Position deletes
        // committed to the table in this path are used only to delete rows from data files that are
        // being added in this commit. There is no way for data files added along with the delete
        // files to be concurrently removed, so there is no need to validate the files referenced by
        // the position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);

        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

        String description = "rowDelta";
        logCommitSummary(summary, description);
        commitOperation(rowDelta, description, newFlinkJobId, operatorId, e.getKey());
      }
    }
  }

  /**
   * Commits one checkpoint of the DV-only write path.
   *
   * <p>Its deletion vectors were merged with the vectors the table held when the checkpoint barrier
   * arrived, and resolve deletes against the primary key index as of the baseline snapshot. They
   * are committed as they are unless something committed since the baseline invalidates them: a
   * data file they reference was removed, a vector was added to one of those files, or a vector
   * they replace was removed. The commit validates exactly that.
   *
   * <p>Such a conflict is not an error of the checkpoint, and failing the job would only replay the
   * same commit, so the vectors are merged again against the current table and the commit is
   * retried with that.
   *
   * <p>When the baseline is no longer an ancestor of the branch, typically because it expired while
   * the job was down, what was committed since cannot be validated. The vectors are merged against
   * the current snapshot instead, provided every data file they reference is still part of it.
   */
  private void commitDvOnlyDelta(
      WriteResult result,
      Long baselineSnapshotId,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    table.refresh();
    boolean asWritten = deletionVectorMerger.isTraceable(baselineSnapshotId);
    Long mergeBaseline =
        asWritten
            ? baselineSnapshotId
            : deletionVectorMerger.replaceBaseline(result, baselineSnapshotId);

    int merges = 0;
    while (true) {
      Iterable<DeleteFile> deletionVectors = Arrays.asList(result.deleteFiles());
      Iterable<DeleteFile> replacedDeletionVectors = Arrays.asList(result.rewrittenDeleteFiles());
      Iterable<? extends CharSequence> referencedDataFiles =
          Arrays.asList(result.referencedDataFiles());
      Long fromSnapshotId = baselineSnapshotId;
      DvOnlyDeletionVectorMerger.Merged merged = null;
      if (!asWritten) {
        table.refresh();
        merged = deletionVectorMerger.merge(result, mergeBaseline);
        merges++;
        deletionVectors = merged.deletionVectors();
        replacedDeletionVectors = merged.replacedDeletionVectors();
        referencedDataFiles = merged.referencedDataFiles();
        fromSnapshotId = merged.head() != null ? merged.head().snapshotId() : null;
      }

      try {
        commitDvOnlyAttempt(
            result,
            deletionVectors,
            replacedDeletionVectors,
            referencedDataFiles,
            fromSnapshotId,
            summary,
            newFlinkJobId,
            operatorId,
            checkpointId);
      } catch (ValidationException e) {
        if (merged != null) {
          DvOnlyDeletionVectorMerger.deleteQuietly(table.io(), merged.deletionVectors());
        }

        if (merges >= MAX_DV_ONLY_COMMIT_ATTEMPTS) {
          throw e;
        }

        LOG.warn(
            "A concurrent commit to table {} branch {} conflicts with the deletion vectors of "
                + "checkpoint {}, merging them again ({} of {} merges done)",
            table.name(),
            branch,
            checkpointId,
            merges,
            MAX_DV_ONLY_COMMIT_ATTEMPTS,
            e);
        asWritten = false;
        continue;
      }

      if (merged != null) {
        // Superseded by the merged vectors, which are the ones that got committed.
        DvOnlyDeletionVectorMerger.deleteQuietly(table.io(), Arrays.asList(result.deleteFiles()));
      }

      return;
    }
  }

  /**
   * Commits the data files of a checkpoint with the given deletion vectors, failing with a {@link
   * ValidationException} when anything committed after {@code fromSnapshotId} conflicts with them.
   */
  private void commitDvOnlyAttempt(
      WriteResult result,
      Iterable<DeleteFile> deletionVectors,
      Iterable<DeleteFile> replacedDeletionVectors,
      Iterable<? extends CharSequence> referencedDataFiles,
      Long fromSnapshotId,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    deletionVectors.forEach(rowDelta::addDeletes);
    replacedDeletionVectors.forEach(rowDelta::removeDeletes);
    if (fromSnapshotId != null) {
      rowDelta.validateFromSnapshot(fromSnapshotId);
    }

    rowDelta.validateDataFilesExist(referencedDataFiles).validateDeletedFiles();

    String description = "rowDelta";
    logCommitSummary(summary, description);
    commitOperation(rowDelta, description, newFlinkJobId, operatorId, checkpointId);
  }

  private void commitOperation(
      SnapshotUpdate<?> operation,
      String description,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {

    snapshotProperties.forEach(operation::set);
    // custom snapshot metadata properties will be overridden if they conflict with internal ones
    // used by the sink.
    operation.set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(SinkUtil.FLINK_JOB_ID, newFlinkJobId);
    operation.set(SinkUtil.OPERATOR_ID, operatorId);
    operation.toBranch(branch);

    long startNano = System.nanoTime();
    operation.commit(); // abort is automatically called if this fails.
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    LOG.info(
        "Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
        description,
        table.name(),
        branch,
        checkpointId,
        durationMs);
    if (committerMetrics != null) {
      committerMetrics.commitDuration(durationMs);
    }
  }

  @Override
  public void close() throws IOException {
    if (tableLoader.isOpen()) {
      tableLoader.close();
    }

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }
}
