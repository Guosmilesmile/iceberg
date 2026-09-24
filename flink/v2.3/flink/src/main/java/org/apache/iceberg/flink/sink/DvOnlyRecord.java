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

import java.io.Serializable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;

/**
 * Unit of work in the pre-commit topology of the DV-only write path. It carries both the row
 * changes the primary key index is built from and the commands that keep that index in step with
 * the table.
 *
 * <p>Records are routed twice: by {@link #filePath()} into the operator that owns the rows of a
 * data file, and by {@link #key()} into the operator that owns the positions of an equality key.
 * Each type therefore fills only the fields its stage needs, and the type states which ones those
 * are.
 *
 * @param type what the record asks for
 * @param key serialized equality field values, set for everything routed by key
 * @param position row location, set for the two row types
 * @param filePath data file the record belongs to, set for everything routed by file
 * @param readTask serialized {@link PkIndexReadTask}, set for {@link Type#READ_FILE}
 * @param checkpointId checkpoint that produced the record, or {@link #NO_CHECKPOINT} when it did
 *     not come from a writer
 */
@Internal
public record DvOnlyRecord(
    Type type,
    SerializedEqualityValues key,
    DVPosition position,
    String filePath,
    byte[] readTask,
    long checkpointId)
    implements Serializable {

  /** Used by records that do not belong to a checkpoint of this job. */
  public static final long NO_CHECKPOINT = -1L;

  public enum Type {
    /** Delete of a key that no row written in the same checkpoint matched. */
    DELETE,
    /** Row written during the current checkpoint, indexed once that checkpoint resolves. */
    ADD_ROW,
    /** Row the table already holds, indexed right away. */
    BOOTSTRAP_ROW,
    /** Drops the positions a key holds in one data file, because that file left the table. */
    DROP_POSITIONS,
    /** Asks for the live rows of one data file to be reported as {@link #BOOTSTRAP_ROW}. */
    READ_FILE,
    /** Reports that a data file left the table, so the keys it held have to be dropped. */
    DROP_FILE
  }

  public static DvOnlyRecord delete(SerializedEqualityValues key, long checkpointId) {
    return new DvOnlyRecord(Type.DELETE, key, null, null, null, checkpointId);
  }

  public static DvOnlyRecord addRow(PkIndexEntry entry, long checkpointId) {
    return new DvOnlyRecord(
        Type.ADD_ROW,
        entry.key(),
        entry.position(),
        entry.position().dataFilePath(),
        null,
        checkpointId);
  }

  public static DvOnlyRecord bootstrapRow(PkIndexEntry entry) {
    return new DvOnlyRecord(
        Type.BOOTSTRAP_ROW,
        entry.key(),
        entry.position(),
        entry.position().dataFilePath(),
        null,
        NO_CHECKPOINT);
  }

  public static DvOnlyRecord dropPositions(SerializedEqualityValues key, String filePath) {
    return new DvOnlyRecord(Type.DROP_POSITIONS, key, null, filePath, null, NO_CHECKPOINT);
  }

  public static DvOnlyRecord readFile(String filePath, byte[] readTask) {
    return new DvOnlyRecord(Type.READ_FILE, null, null, filePath, readTask, NO_CHECKPOINT);
  }

  public static DvOnlyRecord dropFile(String filePath) {
    return new DvOnlyRecord(Type.DROP_FILE, null, null, filePath, null, NO_CHECKPOINT);
  }
}
