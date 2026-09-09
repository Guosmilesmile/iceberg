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
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The temporary manifests holding the files of one checkpoint.
 *
 * <p>A manifest is written for a single partition spec, so delete files are spread over one
 * manifest per spec they belong to. Only the DV-only write path produces more than one, because its
 * deletion vectors reference data files of whatever spec those were written with.
 */
@Internal
public class DeltaManifests {

  private static final CharSequence[] EMPTY_REF_DATA_FILES = new CharSequence[0];

  private final ManifestFile dataManifest;
  private final List<ManifestFile> deleteManifests;
  private final List<ManifestFile> rewrittenDeleteManifests;
  private final CharSequence[] referencedDataFiles;
  private final Long baselineSnapshotId;

  DeltaManifests(ManifestFile dataManifest, ManifestFile deleteManifest) {
    this(dataManifest, deleteManifest, EMPTY_REF_DATA_FILES);
  }

  DeltaManifests(
      ManifestFile dataManifest, ManifestFile deleteManifest, CharSequence[] referencedDataFiles) {
    this(
        dataManifest,
        deleteManifest != null ? ImmutableList.of(deleteManifest) : ImmutableList.of(),
        ImmutableList.of(),
        referencedDataFiles,
        null);
  }

  DeltaManifests(
      ManifestFile dataManifest,
      List<ManifestFile> deleteManifests,
      List<ManifestFile> rewrittenDeleteManifests,
      CharSequence[] referencedDataFiles,
      Long baselineSnapshotId) {
    Preconditions.checkNotNull(deleteManifests, "Delete manifests shouldn't be null.");
    Preconditions.checkNotNull(
        rewrittenDeleteManifests, "Rewritten delete manifests shouldn't be null.");
    Preconditions.checkNotNull(referencedDataFiles, "Referenced data files shouldn't be null.");

    this.dataManifest = dataManifest;
    this.deleteManifests = ImmutableList.copyOf(deleteManifests);
    this.rewrittenDeleteManifests = ImmutableList.copyOf(rewrittenDeleteManifests);
    this.referencedDataFiles = referencedDataFiles;
    this.baselineSnapshotId = baselineSnapshotId;
  }

  ManifestFile dataManifest() {
    return dataManifest;
  }

  /** Delete manifests of the checkpoint, one per partition spec. */
  List<ManifestFile> deleteManifests() {
    return deleteManifests;
  }

  /**
   * Delete files superseded by the ones in {@link #deleteManifests()}, to drop on commit, one
   * manifest per partition spec.
   */
  List<ManifestFile> rewrittenDeleteManifests() {
    return rewrittenDeleteManifests;
  }

  CharSequence[] referencedDataFiles() {
    return referencedDataFiles;
  }

  /**
   * Snapshot the primary key index reflected when the deletes in {@link #deleteManifests()} were
   * resolved, or null when the branch had none or the sink does not resolve deletes itself.
   */
  Long baselineSnapshotId() {
    return baselineSnapshotId;
  }

  public List<ManifestFile> manifests() {
    List<ManifestFile> manifests =
        Lists.newArrayListWithCapacity(
            1 + deleteManifests.size() + rewrittenDeleteManifests.size());
    if (dataManifest != null) {
      manifests.add(dataManifest);
    }

    manifests.addAll(deleteManifests);
    manifests.addAll(rewrittenDeleteManifests);
    return manifests;
  }
}
