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
package org.apache.iceberg.flink.data;

import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.BaseFormatModelTests;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

public class TestFlinkFormatModel extends BaseFormatModelTests<RowData> {

  @Override
  protected Class<RowData> engineType() {
    return RowData.class;
  }

  @Override
  protected Object engineSchema(Schema schema) {
    return FlinkSchemaUtil.convert(schema);
  }

  @Override
  protected RowData convertToEngine(Record record, Schema schema) {
    return RowDataConverter.convert(schema, record);
  }

  @Override
  protected void assertEquals(Schema schema, List<RowData> expected, List<RowData> actual) {
    TestHelpers.assertRows(actual, expected, FlinkSchemaUtil.convert(schema));
  }

  @Override
  protected Object convertConstantToEngine(Types.NestedField field, Object value) {
    return RowDataUtil.convertConstant(field.type(), value);
  }

  @Override
  protected <D> List<D> convertToPartitionIdentity(
      List<RowData> actual, int index, Class<D> clazz) {
    List<D> partitionIdentity = Lists.newArrayList();
    for (RowData row : actual) {
      Object object = ((GenericRowData) row).getField(0);
      if (object instanceof PartitionData partition) {
        partitionIdentity.add(partition.get(index, clazz));
      } else {
        throw new IllegalArgumentException("Not a partition data");
      }
    }

    return partitionIdentity;
  }
}
