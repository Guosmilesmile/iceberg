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
 * A single equality key together with what should happen to it, routed to {@link
 * DvOnlyResolveOperator} by {@link #key()}.
 *
 * <p>{@link Type#DELETE} asks the operator to mark the row currently indexed for the key as
 * deleted. {@link Type#ADD_ROW} reports a newly written row and carries its {@link #position()}.
 * {@link Type#BOOTSTRAP_ROW} reports a row that was already committed when the job started; it
 * differs from {@code ADD_ROW} in that it is applied to the index immediately rather than after the
 * current checkpoint resolves.
 *
 * @param type what to do with the key
 * @param key serialized equality field values
 * @param position row location, set for the two row types and null for {@link Type#DELETE}
 * @param checkpointId checkpoint that produced the record, or {@link #NO_CHECKPOINT} for bootstrap
 */
@Internal
public record DvOnlyRecord(
    Type type, SerializedEqualityValues key, DVPosition position, long checkpointId)
    implements Serializable {

  /** Used by bootstrap records, which do not belong to a checkpoint of this job. */
  public static final long NO_CHECKPOINT = -1L;

  public enum Type {
    DELETE,
    ADD_ROW,
    BOOTSTRAP_ROW
  }

  public static DvOnlyRecord delete(SerializedEqualityValues key, long checkpointId) {
    return new DvOnlyRecord(Type.DELETE, key, null, checkpointId);
  }

  public static DvOnlyRecord addRow(PkIndexEntry entry, long checkpointId) {
    return new DvOnlyRecord(Type.ADD_ROW, entry.key(), entry.position(), checkpointId);
  }

  public static DvOnlyRecord bootstrapRow(PkIndexEntry entry) {
    return new DvOnlyRecord(Type.BOOTSTRAP_ROW, entry.key(), entry.position(), NO_CHECKPOINT);
  }
}
