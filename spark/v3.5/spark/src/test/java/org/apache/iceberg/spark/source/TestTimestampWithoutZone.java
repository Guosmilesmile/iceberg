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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.Files.localOutput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.GenericsHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTimestampWithoutZone extends TestBase {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    TestTimestampWithoutZone.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestTimestampWithoutZone.spark;
    TestTimestampWithoutZone.spark = null;
    currentSpark.stop();
  }

  @TempDir private Path temp;

  @Parameter(index = 0)
  private FileFormat fileFormat;

  @Parameter(index = 1)
  private boolean vectorized;

  @Parameters(name = "format = {0}, vectorized = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.PARQUET, false},
      {FileFormat.PARQUET, true},
      {FileFormat.AVRO, false}
    };
  }

  private File parent = null;
  private File unpartitioned = null;
  private List<Record> records = null;

  @BeforeEach
  public void writeUnpartitionedTable() throws IOException {
    this.parent = temp.resolve("TestTimestampWithoutZone").toFile();
    this.unpartitioned = new File(parent, "unpartitioned");
    File dataFolder = new File(unpartitioned, "data");
    assertThat(dataFolder.mkdirs()).as("Mkdir should succeed").isTrue();

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), unpartitioned.toString());
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    File testFile = new File(dataFolder, fileFormat.addExtension(UUID.randomUUID().toString()));

    // create records using the table's schema
    this.records = testRecords(tableSchema);

    try (FileAppender<Record> writer =
        new GenericAppenderFactory(tableSchema).newAppender(localOutput(testFile), fileFormat)) {
      writer.addAll(records);
    }

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(records.size())
            .withFileSizeInBytes(testFile.length())
            .withPath(testFile.toString())
            .build();

    table.newAppend().appendFile(file).commit();
  }

  @TestTemplate
  public void testUnpartitionedTimestampWithoutZone() {
    assertEqualsSafe(SCHEMA.asStruct(), records, read(unpartitioned.toString(), vectorized));
  }

  @TestTemplate
  public void testUnpartitionedTimestampWithoutZoneProjection() {
    Schema projection = SCHEMA.select("id", "ts");
    assertEqualsSafe(
        projection.asStruct(),
        records.stream().map(r -> projectFlat(projection, r)).collect(Collectors.toList()),
        read(unpartitioned.toString(), vectorized, "id", "ts"));
  }

  @TestTemplate
  public void testUnpartitionedTimestampWithoutZoneAppend() {
    spark
        .read()
        .format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
        .load(unpartitioned.toString())
        .write()
        .format("iceberg")
        .mode(SaveMode.Append)
        .save(unpartitioned.toString());

    assertEqualsSafe(
        SCHEMA.asStruct(),
        Stream.concat(records.stream(), records.stream()).collect(Collectors.toList()),
        read(unpartitioned.toString(), vectorized));
  }

  private static Record projectFlat(Schema projection, Record record) {
    Record result = GenericRecord.create(projection);
    List<Types.NestedField> fields = projection.asStruct().fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      result.set(i, record.getField(field.name()));
    }
    return result;
  }

  public static void assertEqualsSafe(
      Types.StructType struct, List<Record> expected, List<Row> actual) {
    assertThat(actual).as("Number of results should match expected").hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i += 1) {
      GenericsHelpers.assertEqualsSafe(struct, expected.get(i), actual.get(i));
    }
  }

  private List<Record> testRecords(Schema schema) {
    return Lists.newArrayList(
        record(schema, 0L, parseToLocal("2017-12-22T09:20:44.294658"), "junction"),
        record(schema, 1L, parseToLocal("2017-12-22T07:15:34.582910"), "alligator"),
        record(schema, 2L, parseToLocal("2017-12-22T06:02:09.243857"), "forrest"),
        record(schema, 3L, parseToLocal("2017-12-22T03:10:11.134509"), "clapping"),
        record(schema, 4L, parseToLocal("2017-12-22T00:34:00.184671"), "brush"),
        record(schema, 5L, parseToLocal("2017-12-21T22:20:08.935889"), "trap"),
        record(schema, 6L, parseToLocal("2017-12-21T21:55:30.589712"), "element"),
        record(schema, 7L, parseToLocal("2017-12-21T17:31:14.532797"), "limited"),
        record(schema, 8L, parseToLocal("2017-12-21T15:21:51.237521"), "global"),
        record(schema, 9L, parseToLocal("2017-12-21T15:02:15.230570"), "goldfish"));
  }

  private static List<Row> read(String table, boolean vectorized) {
    return read(table, vectorized, "*");
  }

  private static List<Row> read(
      String table, boolean vectorized, String select0, String... selectN) {
    Dataset<Row> dataset =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
            .load(table)
            .select(select0, selectN);
    return dataset.collectAsList();
  }

  private static LocalDateTime parseToLocal(String timestamp) {
    return LocalDateTime.parse(timestamp);
  }

  private static Record record(Schema schema, Object... values) {
    Record rec = GenericRecord.create(schema);
    for (int i = 0; i < values.length; i += 1) {
      rec.set(i, values[i]);
    }
    return rec;
  }
}
