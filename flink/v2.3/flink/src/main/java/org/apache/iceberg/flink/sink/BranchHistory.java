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

import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;

/** What the DV-only write path needs to know about the snapshots committed to a branch. */
final class BranchHistory {

  private BranchHistory() {}

  /**
   * Whether the snapshots committed after {@code fromSnapshotId} up to {@code head} can still be
   * enumerated. They cannot once {@code fromSnapshotId} stopped being an ancestor of the branch,
   * typically because it expired.
   *
   * @param head the branch head, or null when the branch has no snapshot
   * @param fromSnapshotId the snapshot to start after, or null for the start of the history
   */
  static boolean isTraceable(Table table, Snapshot head, Long fromSnapshotId) {
    return fromSnapshotId == null
        || head == null
        || SnapshotUtil.isAncestorOf(head.snapshotId(), fromSnapshotId, table::snapshot);
  }

  /**
   * Returns the snapshots committed after {@code fromSnapshotId} up to and including {@code head},
   * oldest first.
   *
   * @param head the branch head
   * @param fromSnapshotId the snapshot to start after, or null for the start of the history; has to
   *     be {@link #isTraceable traceable}
   */
  static List<Snapshot> committedSince(Table table, Snapshot head, Long fromSnapshotId) {
    Preconditions.checkState(
        isTraceable(table, head, fromSnapshotId),
        "Snapshot %s is no longer an ancestor of snapshot %s of table %s",
        fromSnapshotId,
        head.snapshotId(),
        table.name());
    return Lists.reverse(
        Lists.newArrayList(
            SnapshotUtil.ancestorsBetween(head.snapshotId(), fromSnapshotId, table::snapshot)));
  }

  /** Whether a snapshot added or removed data files, judged from its summary. */
  static boolean changesDataFiles(Snapshot snapshot) {
    return addedDataFiles(snapshot) > 0 || removedDataFiles(snapshot) > 0;
  }

  static long addedDataFiles(Snapshot snapshot) {
    return PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, 0);
  }

  static long removedDataFiles(Snapshot snapshot) {
    return PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.DELETED_FILES_PROP, 0);
  }

  static SnapshotChanges changes(Table table, Snapshot snapshot) {
    return SnapshotChanges.builderFor(table).snapshot(snapshot).build();
  }
}
