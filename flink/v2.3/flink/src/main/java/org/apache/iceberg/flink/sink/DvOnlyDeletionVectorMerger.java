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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectors;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectors.FilePositions;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges the deletion vectors of one checkpoint again, against the state the branch is in right
 * now. Used when committing them as written failed, because a concurrent commit changed a data file
 * they reference, or could not be validated, because the baseline snapshot expired.
 *
 * <p>Only the positions the checkpoint added are carried over, that is each vector minus the one it
 * was meant to replace. They are merged with the vector each data file carries now, which covers a
 * vector the previous checkpoint committed in the meantime.
 *
 * <p>The positions were resolved against the primary key index as of the baseline snapshot. A data
 * file that another operation removed after that snapshot, typically a compaction, no longer
 * exists, so the deletes aimed at it are resolved again: the equality keys of the deleted rows are
 * read from the removed file and every row with one of those keys is deleted from the files that
 * replaced it. A delete of a key removes every row of that key written before it, and the
 * replacements only hold rows committed before the checkpoint, so this is the outcome the original
 * positions would have had.
 *
 * <p>The replacements of a file are the files added by the snapshot that removed it, followed
 * through any later snapshot that removed those in turn. A rewrite keeps every row in its
 * partition, so a replacement of the same spec but another partition cannot hold the rows and is
 * not read.
 */
class DvOnlyDeletionVectorMerger {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyDeletionVectorMerger.class);

  private final Table table;
  private final String branch;
  private final PkIndexFileReader reader;
  private final DeletionVectors deletionVectors;
  private final OutputFileFactory fileFactory;

  DvOnlyDeletionVectorMerger(
      Table table, String branch, Set<Integer> equalityFieldIds, OutputFileFactory fileFactory) {
    this.table = table;
    this.branch = branch;
    this.reader = new PkIndexFileReader(table, equalityFieldIds);
    this.deletionVectors = new DeletionVectors(table);
    this.fileFactory = fileFactory;
  }

  /**
   * Whether the snapshots committed since the baseline can still be enumerated, which is what
   * validating the vectors as written and tracing removed data files rely on.
   */
  boolean isTraceable(Long baselineSnapshotId) {
    return BranchHistory.isTraceable(table, table.snapshot(branch), baselineSnapshotId);
  }

  /**
   * Returns the snapshot to merge a checkpoint against when its baseline is no longer traceable:
   * the current one, provided every data file the checkpoint deletes from is still part of it. A
   * data file removed in between cannot be traced to the files that replaced it, so the deletes
   * aimed at it cannot be resolved again.
   *
   * @param result the files of the checkpoint, as written by {@link DvOnlyDVWriterOperator}
   * @param baselineSnapshotId the baseline that is no longer traceable, for the error message
   */
  Long replaceBaseline(WriteResult result, Long baselineSnapshotId) {
    Snapshot head = table.snapshot(branch);
    Map<String, FilePositions> files = Maps.newHashMap();
    for (DeleteFile vector : result.deleteFiles()) {
      files.put(
          vector.referencedDataFile(),
          FilePositions.forPartition(vector.specId(), vector.partition()));
    }

    Set<String> missing = Sets.newHashSet(files.keySet());
    missing.removeAll(deletionVectors.findLive(head, files));
    Preconditions.checkState(
        missing.isEmpty(),
        "Cannot commit deletion vectors to branch '%s' of table %s: snapshot %s the deletes were "
            + "resolved against is no longer an ancestor of the branch, most likely because it "
            + "expired, and data files %s were removed since. The rows these deletes are aimed at "
            + "can no longer be traced. Retain snapshots for longer than the sink takes to commit.",
        branch,
        table.name(),
        baselineSnapshotId,
        missing);

    LOG.warn(
        "Snapshot {} the deletes were resolved against is no longer an ancestor of branch '{}' of "
            + "table {}, but every data file they are aimed at is still part of it; merging them "
            + "against the current snapshot",
        baselineSnapshotId,
        branch,
        table.name());
    return head != null ? head.snapshotId() : null;
  }

  /**
   * Computes the deletion vectors to commit for one checkpoint. The table has to be refreshed by
   * the caller, since the result is only valid against the state it was computed from.
   *
   * @param result the files of the checkpoint, as written by {@link DvOnlyDVWriterOperator}
   * @param baselineSnapshotId snapshot the deletes were resolved against, or null when the branch
   *     had none; has to be {@link #isTraceable traceable}
   */
  Merged merge(WriteResult result, Long baselineSnapshotId) {
    Snapshot head = table.snapshot(branch);
    Map<String, FilePositions> pending = addedPositions(result);
    Rewrites rewrites = rewritesSince(baselineSnapshotId, head);

    Set<String> moved = Sets.newHashSet(pending.keySet());
    moved.retainAll(rewrites.removedBy.keySet());
    if (!moved.isEmpty()) {
      resolveAgain(moved, pending, rewrites);
    }

    Map<String, DeleteFile> existing = deletionVectors.findExisting(head, pending);
    return new Merged(head, deletionVectors.write(fileFactory, pending, existing));
  }

  /** Positions each vector of the checkpoint adds to the vector it was meant to replace. */
  private Map<String, FilePositions> addedPositions(WriteResult result) {
    Map<String, DeleteFile> replaced = Maps.newHashMap();
    for (DeleteFile deleteFile : result.rewrittenDeleteFiles()) {
      replaced.put(deleteFile.referencedDataFile(), deleteFile);
    }

    Map<String, FilePositions> pending = Maps.newLinkedHashMap();
    for (DeleteFile vector : result.deleteFiles()) {
      Preconditions.checkState(
          ContentFileUtil.isDV(vector),
          "Expected a deletion vector, but found delete file %s",
          vector.location());
      String dataFilePath = vector.referencedDataFile();
      Preconditions.checkState(
          !pending.containsKey(dataFilePath),
          "Found more than one deletion vector for data file %s",
          dataFilePath);
      DeleteFile previous = replaced.get(dataFilePath);
      PositionDeleteIndex previousPositions =
          previous != null ? deletionVectors.load(previous) : null;
      FilePositions file = FilePositions.forPartition(vector.specId(), vector.partition());
      deletionVectors
          .load(vector)
          .forEach(
              position -> {
                if (previousPositions == null || !previousPositions.isDeleted(position)) {
                  file.add(position);
                }
              });
      pending.put(dataFilePath, file);
    }

    return pending;
  }

  /** The data files removed by the snapshots committed after the baseline, and what they added. */
  private Rewrites rewritesSince(Long baselineSnapshotId, Snapshot head) {
    Rewrites rewrites = new Rewrites();
    if (head == null) {
      return rewrites;
    }

    Preconditions.checkState(
        isTraceable(baselineSnapshotId),
        "Snapshot %s the deletes were resolved against is no longer an ancestor of branch '%s' of "
            + "table %s",
        baselineSnapshotId,
        branch,
        table.name());

    for (Snapshot snapshot : BranchHistory.committedSince(table, head, baselineSnapshotId)) {
      if (BranchHistory.removedDataFiles(snapshot) == 0) {
        continue;
      }

      SnapshotChanges changes = BranchHistory.changes(table, snapshot);
      for (DataFile file : changes.removedDataFiles()) {
        rewrites.removed.put(file.location(), file);
        rewrites.removedBy.put(file.location(), snapshot.snapshotId());
      }

      List<DataFile> added = Lists.newArrayList(changes.addedDataFiles());
      rewrites.addedBy.put(snapshot.snapshotId(), added);
    }

    return rewrites;
  }

  /**
   * Replaces the positions pending for data files that left the table with the positions of the
   * rows holding the same keys in the files that replaced them. Rows of the replacements that are
   * already deleted may be reported again, which is harmless since deleting a position is
   * idempotent.
   */
  private void resolveAgain(
      Set<String> moved, Map<String, FilePositions> pending, Rewrites rewrites) {
    Map<String, DataFile> replacements = Maps.newLinkedHashMap();
    Map<String, Set<SerializedEqualityValues>> keysByReplacement = Maps.newHashMap();
    long keyCount = 0;
    for (String path : moved) {
      Set<SerializedEqualityValues> keys = Sets.newHashSet();
      collectKeys(rewrites.removed.get(path), pending.remove(path), keys);
      keyCount += keys.size();
      if (keys.isEmpty()) {
        continue;
      }

      for (DataFile replacement : replacementsOf(rewrites.removed.get(path), rewrites)) {
        replacements.put(replacement.location(), replacement);
        keysByReplacement
            .computeIfAbsent(replacement.location(), location -> Sets.newHashSet())
            .addAll(keys);
      }
    }

    long resolved = 0;
    for (DataFile replacement : replacements.values()) {
      Set<SerializedEqualityValues> keys = keysByReplacement.get(replacement.location());
      FilePositions file =
          FilePositions.forPartition(replacement.specId(), replacement.partition());
      reader.read(
          new PkIndexReadTask(replacement, ImmutableList.of()),
          entry -> {
            if (keys.contains(entry.key())) {
              file.add(entry.position().position());
            }
          });

      if (!file.positions().isEmpty()) {
        resolved += file.positions().getLongCardinality();
        pending.merge(
            replacement.location(),
            file,
            (current, added) -> {
              current.positions().or(added.positions());
              return current;
            });
      }
    }

    LOG.info(
        "Resolved again the deletes of {} key(s) aimed at {} data file(s) that left branch '{}' of "
            + "table {}: read {} replacement(s), which removed {} row(s)",
        keyCount,
        moved.size(),
        branch,
        table.name(),
        replacements.size(),
        resolved);
  }

  /**
   * The files still part of the branch that hold the rows of a removed file: the files the removing
   * snapshot added in the same partition, and in turn the replacements of those that were removed
   * as well.
   */
  private List<DataFile> replacementsOf(DataFile removedFile, Rewrites rewrites) {
    List<DataFile> replacements = Lists.newArrayList();
    Set<String> visited = Sets.newHashSet(removedFile.location());
    Deque<DataFile> toTrace = new ArrayDeque<>();
    toTrace.add(removedFile);
    while (!toTrace.isEmpty()) {
      DataFile traced = toTrace.poll();
      Long removedBy = rewrites.removedBy.get(traced.location());
      for (DataFile added : rewrites.addedBy.getOrDefault(removedBy, ImmutableList.of())) {
        if (!mayHoldRowsOf(added, traced) || !visited.add(added.location())) {
          continue;
        }

        if (rewrites.removedBy.containsKey(added.location())) {
          toTrace.add(added);
        } else {
          replacements.add(added);
        }
      }
    }

    return replacements;
  }

  /** Whether a file added by a rewrite may hold rows of a file it removed. */
  private boolean mayHoldRowsOf(DataFile added, DataFile removedFile) {
    if (added.specId() != removedFile.specId()) {
      return true;
    }

    StructLikeWrapper partition =
        StructLikeWrapper.forType(table.specs().get(added.specId()).partitionType());
    return partition.copyFor(added.partition()).equals(partition.copyFor(removedFile.partition()));
  }

  private void collectKeys(
      DataFile removedFile, FilePositions positions, Set<SerializedEqualityValues> keys) {
    try {
      reader.read(
          new PkIndexReadTask(removedFile, ImmutableList.of()),
          entry -> {
            if (positions.positions().contains(entry.position().position())) {
              keys.add(entry.key());
            }
          });
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          String.format(
              "Cannot resolve again the deletes aimed at data file %s, which left branch '%s' of "
                  + "table %s after the deletes were resolved: the file is no longer readable",
              removedFile.location(), branch, table.name()),
          e);
    }
  }

  /** Deletes files that were written but are not referenced by any commit. */
  static void deleteQuietly(FileIO io, Collection<DeleteFile> files) {
    Set<String> locations = Sets.newHashSet();
    files.forEach(file -> locations.add(file.location()));
    CatalogUtil.deleteFiles(io, locations, "deletion vector");
  }

  /** The data files removed after the baseline, and the files each removing snapshot added. */
  private static class Rewrites {
    private final Map<String, DataFile> removed = Maps.newHashMap();
    private final Map<String, Long> removedBy = Maps.newHashMap();
    private final Map<Long, List<DataFile>> addedBy = Maps.newHashMap();
  }

  /** The deletion vectors to commit for one checkpoint and the snapshot they were computed from. */
  static class Merged {
    private final Snapshot head;
    private final DeleteWriteResult result;

    private Merged(Snapshot head, DeleteWriteResult result) {
      this.head = head;
      this.result = result;
    }

    /** Snapshot the vectors were merged against, or null when the branch has none. */
    Snapshot head() {
      return head;
    }

    List<DeleteFile> deletionVectors() {
      return result.deleteFiles();
    }

    /** Vectors the ones in {@link #deletionVectors()} replace. */
    List<DeleteFile> replacedDeletionVectors() {
      return result.rewrittenDeleteFiles();
    }

    Iterable<CharSequence> referencedDataFiles() {
      return result.referencedDataFiles();
    }
  }
}
