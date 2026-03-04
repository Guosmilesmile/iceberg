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
package org.apache.iceberg.parquet;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.util.List;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A abstract visitor for schema inference.
 *
 * @param <S> the schema type
 * @param <R> the row type
 */
public abstract class AbstractSchemaInferenceVisitor<S, R> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSchemaInferenceVisitor.class);

  private final List<R> bufferedRows;
  private final S schema;
  private final VariantShreddingAnalyzer<R> analyzer;

  protected AbstractSchemaInferenceVisitor(List<R> bufferedRows, S schema) {
    this.bufferedRows = bufferedRows;
    this.schema = schema;
    this.analyzer = createAnalyzer();
  }

  public Type buildMessage(MessageType message, List<Type> fields) {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (Type field : fields) {
      if (field != null) {
        builder.addField(field);
      }
    }

    return builder.named(message.getName());
  }

  public Type buildStruct(GroupType struct, List<Type> fields) {
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

  public Type buildList(GroupType array, Type element) {
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

  public Type buildMap(GroupType map, Type key, Type value) {
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

  public Type buildVariant(GroupType variant, int variantFieldIndex) {
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

  protected int getFieldIndexCommon(String[] path, FieldNameResolver<S> resolver) {
    if (path == null || path.length == 0) {
      return -1;
    }

    if (path.length == 1) {
      String fieldName = path[0];
      return resolver.resolveFieldIndex(schema, fieldName);
    } else {
      // TODO: Implement full nested field resolution
      LOG.warn("Nested variant shredding is not supported. Path: {}", String.join(".", path));
    }

    return -1;
  }

  protected abstract VariantShreddingAnalyzer<R> createAnalyzer();

  public abstract int getFieldIndex(String[] path);

  @FunctionalInterface
  public interface FieldNameResolver<S> {
    int resolveFieldIndex(S schema, String fieldName);
  }
}
