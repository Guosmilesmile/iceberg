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
package org.apache.iceberg.flink;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.reader.ReaderUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;

class TestFlinkVariantType extends CatalogTestBase {

    private static final String TABLE_NAME = "test_table";
    private Table icebergTable;
    @TempDir
    private Path warehouseDir;

    @Parameter(index = 2)
    protected boolean variantShreddingEnable;

    @Parameters(name = "catalogName={0}, baseNamespace={1}, variantShreddingEnable={2}")
    protected static List<Object[]> parameters() {
        List<Object[]> parameters = Lists.newArrayList();
        parameters.add(new Object[]{"testhadoop", Namespace.empty(), true});
        //            ,
        //        new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1")}
        //    );
        return parameters;
    }

    @Override
    @BeforeEach
    public void before() {
        super.before();
        sql("CREATE DATABASE %s", flinkDatabase);
        sql("USE CATALOG %s", catalogName);
        sql("USE %s", DATABASE);
        sql(
                "CREATE TABLE %s (id int NOT NULL, data variant NOT NULL) with ('write.format.default'='%s','format-version'='3','write.parquet.variant-shredding.enabled'='%s')",
                TABLE_NAME, FileFormat.PARQUET.name(), variantShreddingEnable);
        icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    public void testInsertVariantFromFlink() throws Exception {
        sql("INSERT INTO %s VALUES (1, PARSE_JSON('%s'))", TABLE_NAME, "{\"KeyA\":\"city\"}");
        List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
        assertThat(records).hasSize(1);
        Object field = records.get(0).getField("data");
        assertThat(field).isInstanceOf(Variant.class);
        Variant variant = (Variant) field;
        assertThat(variant.metadata().get(0)).isEqualTo("KeyA");
        assertThat(((VariantPrimitive<String>) variant.value().asObject().get("KeyA")).get())
                .isEqualTo("city");
    }

    @TestTemplate
    public void testReadVariantFromFlink() throws Exception {
        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        VariantMetadata metadata = Variants.metadata("name", "age");
        ShreddedObject shreddedObject = Variants.object(metadata);
        shreddedObject.put("name", Variants.of("John Doe"));
        shreddedObject.put("age", Variants.of((byte) 30));
        builder.add(
                GenericRecord.create(icebergTable.schema())
                        .copy("id", 1, "data", Variant.of(metadata, shreddedObject)));
        new GenericAppenderHelper(icebergTable, FileFormat.PARQUET, warehouseDir)
                .appendToTable(builder.build());
        icebergTable.refresh();

        List<GenericRowData> genericRowData = Lists.newArrayList();
        try (CloseableIterable<CombinedScanTask> combinedScanTasks =
                     icebergTable.newScan().planTasks()) {
            for (CombinedScanTask combinedScanTask : combinedScanTasks) {
                try (DataIterator<RowData> dataIterator =
                             ReaderUtil.createDataIterator(
                                     combinedScanTask, icebergTable.schema(), icebergTable.schema())) {
                    while (dataIterator.hasNext()) {
                        GenericRowData rowData = (GenericRowData) dataIterator.next();
                        genericRowData.add(rowData);
                    }
                }
            }
        }

        assertThat(genericRowData).hasSize(1);
        assertThat(genericRowData.get(0).getField(1)).isInstanceOf(BinaryVariant.class);
        BinaryVariant binaryVariant = (BinaryVariant) genericRowData.get(0).getField(1);
        assertThat(binaryVariant.getField("name").getString()).isEqualTo("John Doe");
        assertThat(binaryVariant.getField("age").getByte()).isEqualTo((byte) 30);
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    public void testInsertShreddingVariantFromFlink() throws Exception {
        sql(
                "INSERT INTO %s VALUES (1, PARSE_JSON('%s')),(2, PARSE_JSON('%s')),(3, PARSE_JSON('%s'))",
                TABLE_NAME,
                "{\"KeyA\":\"city\"}",
                "{\"KeyA\":\"city1\",\"KeyB\":\"country\"}",
                "{\"KeyC\":\"province\"}");
        List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
        assertThat(records).hasSize(3);
        System.out.println("xxx");
    }

    @TestTemplate
    public void testConsistentType() throws IOException {
        String values =
                "(1, parse_json('{\"name\": \"Alice\", \"age\": 30}')),"
                        + " (2, parse_json('{\"name\": \"Bob\", \"age\": 25}')),"
                        + " (3, parse_json('{\"name\": \"Charlie\", \"age\": 35}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType name =
                field(
                        "name",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
        GroupType age =
                field(
                        "age",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, objectFields(age, name));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testExcludingNullValue() throws IOException {
        String values =
                "(1, parse_json('{\"name\": \"Alice\", \"age\": 30, \"dummy\": null}')),"
                        + " (2, parse_json('{\"name\": \"Bob\", \"age\": 25}')),"
                        + " (3, parse_json('{\"name\": \"Charlie\", \"age\": 35}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType name =
                field(
                        "name",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
        GroupType age =
                field(
                        "age",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, objectFields(age, name));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }


    @TestTemplate
    public void testPrimitiveType() throws IOException {
        String values = "(1, parse_json('123')), (2, parse_json('\"abc\"')), (3, parse_json('12'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType address =
                variant(
                        "data",
                        2,
                        Type.Repetition.REQUIRED,
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
        MessageType expectedSchema = parquetSchema(address);

        List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
        assertThat(records).hasSize(3);
        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testPrimitiveDecimalType() throws IOException {
        String values =
                "(1, parse_json('123.56')), (2, parse_json('\"abc\"')), (3, parse_json('12.56'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType address =
                variant(
                        "data",
                        2,
                        Type.Repetition.REQUIRED,
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(2, 5)));
        MessageType expectedSchema = parquetSchema(address);
        List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
        assertThat(records).hasSize(3);
        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testBooleanType() throws IOException {
        String values =
                "(1, parse_json('{\"active\": true}')),"
                        + " (2, parse_json('{\"active\": false}')),"
                        + " (3, parse_json('{\"active\": true}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType active = field("active", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, objectFields(active));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testDecimalTypeWithInconsistentScales() throws IOException {
        String values =
                "(1, parse_json('{\"price\": 123.456789}')),"
                        + " (2, parse_json('{\"price\": 678.90}')),"
                        + " (3, parse_json('{\"price\": 999.99}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType price =
                field(
                        "price",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(6, 9)));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, objectFields(price));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testDecimalTypeWithConsistentScales() throws IOException {
        String values =
                "(1, parse_json('{\"price\": 123.45}')),"
                        + " (2, parse_json('{\"price\": 678.90}')),"
                        + " (3, parse_json('{\"price\": 999.99}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType price =
                field(
                        "price",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(2, 5)));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, objectFields(price));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testArrayType() throws IOException {
        String values =
                "(1, parse_json('[\"java\", \"scala\", \"python\"]')),"
                        + " (2, parse_json('[\"rust\", \"go\"]')),"
                        + " (3, parse_json('[\"javascript\"]'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType arr =
                list(
                        element(
                                shreddedPrimitive(
                                        PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, arr);
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testNestedArrayType() throws IOException {

        String values =
                "(1, parse_json('{\"tags\": [\"java\", \"scala\", \"python\"]}')),"
                        + " (2, parse_json('{\"tags\": [\"rust\", \"go\"]}')),"
                        + " (3, parse_json('{\"tags\": [\"javascript\"]}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType tags =
                field(
                        "tags",
                        list(
                                element(
                                        shreddedPrimitive(
                                                PrimitiveType.PrimitiveTypeName.BINARY,
                                                LogicalTypeAnnotation.stringType()))));
        GroupType address = variant("data", 2, Type.Repetition.REQUIRED, objectFields(tags));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    @TestTemplate
    public void testNestedObjectType() throws IOException {

        String values =
                "(1, parse_json('{\"location\": {\"city\": \"Seattle\", \"zip\": 98101}, \"tags\": [\"java\", \"scala\", \"python\"]}')),"
                        + " (2, parse_json('{\"location\": {\"city\": \"Portland\", \"zip\": 97201}}')),"
                        + " (3, parse_json('{\"location\": {\"city\": \"NYC\", \"zip\": 10001}}'))";
        sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

        GroupType city =
                field(
                        "city",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
        GroupType zip =
                field(
                        "zip",
                        shreddedPrimitive(
                                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)));
        GroupType location = field("location", objectFields(city, zip));
        GroupType tags =
                field(
                        "tags",
                        list(
                                element(
                                        shreddedPrimitive(
                                                PrimitiveType.PrimitiveTypeName.BINARY,
                                                LogicalTypeAnnotation.stringType()))));

        GroupType address =
                variant("data", 2, Type.Repetition.REQUIRED, objectFields(location, tags));
        MessageType expectedSchema = parquetSchema(address);

        verifyParquetSchema(icebergTable, expectedSchema);
    }

    private void verifyParquetSchema(Table table, MessageType expectedSchema) throws IOException {
        table.refresh();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            assertThat(tasks).isNotEmpty();

            FileScanTask task = tasks.iterator().next();
            String path = task.file().location();

            HadoopInputFile inputFile =
                    HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), new Configuration());

            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                MessageType actualSchema = reader.getFileMetaData().getSchema();
                assertThat(actualSchema).isEqualTo(expectedSchema);
            }
        }
    }

    private static MessageType parquetSchema(Type variantTypes) {
        return org.apache.parquet.schema.Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .id(1)
                .named("id")
                .addFields(variantTypes)
                .named("table");
    }

    private static GroupType variant(String name, int fieldId, Type.Repetition repetition) {
        return org.apache.parquet.schema.Types.buildGroup(repetition)
                .id(fieldId)
                .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("metadata")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("value")
                .named(name);
    }

    private static GroupType variant(
            String name, int fieldId, Type.Repetition repetition, Type shreddedType) {
        checkShreddedType(shreddedType);
        return org.apache.parquet.schema.Types.buildGroup(repetition)
                .id(fieldId)
                .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("metadata")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("value")
                .addField(shreddedType)
                .named(name);
    }

    private static Type shreddedPrimitive(PrimitiveType.PrimitiveTypeName primitive) {
        return optional(primitive).named("typed_value");
    }

    private static Type shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
        return optional(primitive).as(annotation).named("typed_value");
    }

    private static GroupType objectFields(GroupType... fields) {
        for (GroupType fieldType : fields) {
            checkField(fieldType);
        }

        return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.OPTIONAL)
                .addFields(fields)
                .named("typed_value");
    }

    private static GroupType field(String name, Type shreddedType) {
        checkShreddedType(shreddedType);
        return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("value")
                .addField(shreddedType)
                .named(name);
    }

    private static GroupType element(Type shreddedType) {
        return field("element", shreddedType);
    }

    private static GroupType list(GroupType elementType) {
        return org.apache.parquet.schema.Types.optionalList().element(elementType).named("typed_value");
    }

    private static void checkShreddedType(Type shreddedType) {
        Preconditions.checkArgument(
                shreddedType.getName().equals("typed_value"),
                "Invalid shredded type name: %s should be typed_value",
                shreddedType.getName());
        Preconditions.checkArgument(
                shreddedType.isRepetition(Type.Repetition.OPTIONAL),
                "Invalid shredded type repetition: %s should be OPTIONAL",
                shreddedType.getRepetition());
    }

    private static void checkField(GroupType fieldType) {
        Preconditions.checkArgument(
                fieldType.isRepetition(Type.Repetition.REQUIRED),
                "Invalid field type repetition: %s should be REQUIRED",
                fieldType.getRepetition());
    }
}
