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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaInferenceVisitor extends ParquetWithFlinkSchemaVisitor<Type> {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaInferenceVisitor.class);

  private final List<RowData> bufferedRows;
  private final LogicalType flinkSchema;
  private final VariantShreddingAnalyzer analyzer;

  public SchemaInferenceVisitor(List<RowData> bufferedRows, LogicalType flinkSchema) {
    this.bufferedRows = bufferedRows;
    this.flinkSchema = flinkSchema;
    this.analyzer = new VariantShreddingAnalyzer();
  }

  @Override
  public Type message(RowType sStruct, MessageType message, List<Type> fields) {
    MessageTypeBuilder builder = Types.buildMessage();

    for (Type field : fields) {
      if (field != null) {
        builder.addField(field);
      }
    }

    return builder.named(message.getName());
  }

  @Override
  public Type struct(RowType sStruct, GroupType struct, List<Type> fields) {
    Types.GroupBuilder<GroupType> builder = Types.buildGroup(struct.getRepetition());

    if (struct.getId() != null) {
      builder = builder.id(struct.getId().intValue());
    }

    for (Type field : fields) {
      if (field != null) {
        builder = builder.addField(field);
      }
    }

    return builder.named(struct.getName());
  }

  @Override
  public Type primitive(LogicalType sPrimitive, PrimitiveType primitive) {
    return primitive;
  }

  @Override
  public Type list(ArrayType sArray, GroupType array, Type element) {
    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(array.getRepetition()).as(LogicalTypeAnnotation.listType());

    if (array.getId() != null) {
      builder = builder.id(array.getId().intValue());
    }

    if (element != null) {
      builder = builder.addField(element);
    }

    return builder.named(array.getName());
  }

  @Override
  public Type map(MapType sMap, GroupType map, Type key, Type value) {
    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(map.getRepetition()).as(LogicalTypeAnnotation.mapType());

    if (map.getId() != null) {
      builder = builder.id(map.getId().intValue());
    }

    if (key != null) {
      builder = builder.addField(key);
    }
    if (value != null) {
      builder = builder.addField(value);
    }

    return builder.named(map.getName());
  }

  @Override
  public Type variant(VariantType sVariant, GroupType variant) {
    int variantFieldIndex = getFieldIndex(currentPath());

    if (!bufferedRows.isEmpty() && variantFieldIndex >= 0) {
      Type shreddedType = analyzer.analyzeAndCreateSchema(bufferedRows, variantFieldIndex);
      if (shreddedType != null) {
        return Types.buildGroup(variant.getRepetition())
            .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
            .id(variant.getId().intValue())
            .required(BINARY)
            .named("metadata")
            .optional(BINARY)
            .named("value")
            .addField(shreddedType)
            .named(variant.getName());
      }
    }

    return variant;
  }

  private int getFieldIndex(String[] path) {
    if (path == null || path.length == 0) {
      return -1;
    }

    if (path.length == 1) {
      // Top-level field - direct lookup
      RowType rowType = (RowType) flinkSchema;
      String fieldName = path[0];
      for (int i = 0; i < rowType.getFields().size(); i++) {
        if (rowType.getFields().get(i).getName().equals(fieldName)) {
          return i;
        }
      }
    } else {
      // TODO: Implement full nested field resolution
      LOG.warn("Nested variant shredding is not supported. Path: {}", String.join(".", path));
    }

    return -1;
  }
}
