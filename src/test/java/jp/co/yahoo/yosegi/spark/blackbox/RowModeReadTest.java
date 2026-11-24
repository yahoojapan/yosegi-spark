/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.blackbox;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RowModeReadTest {

  private static final String FORMAT = "jp.co.yahoo.yosegi.spark.YosegiFileFormat";
  private static SparkSession spark;

  @TempDir Path tempDir;

  @BeforeAll
  static void setUp() {
    spark = SparkSession.builder().appName("RowModeReadTest").master("local[2]").getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  void readsRowsWhenWholeStageCodegenIsDisabled() {
    assertRowModeRead("spark.sql.codegen.wholeStage", "false");
  }

  @Test
  void readsRowsWhenSchemaExceedsCodegenFieldLimit() {
    assertRowModeRead("spark.sql.codegen.maxFields", "1");
  }

  @Test
  void readsNestedArraysAcrossBatchesAndAfterEarlyLimit() {
    final String path = tempDir.resolve("nested-data").toString();
    // YosegiRecordWriter's default maximum is 50,000 rows per spread.
    final Dataset<Row> source = spark.range(50_001).selectExpr(
        "id",
        "named_struct('values', array(cast(id as int), cast(id + 1 as int))) as nested");
    source.coalesce(1).write().mode(SaveMode.Overwrite).format(FORMAT).save(path);

    final String confKey = "spark.sql.codegen.wholeStage";
    final String previousValue = spark.conf().get(confKey);
    try {
      spark.conf().set(confKey, "false");
      final Dataset<Row> read = spark.read().format(FORMAT).schema(source.schema()).load(path);
      assertTrue(read.queryExecution().executedPlan().toString().contains("Batched: false"));

      final List<Row> limited = read.limit(1).collectAsList();
      assertEquals(1, limited.size());
      assertNestedArray(limited.get(0), 0);

      final List<Row> actual = read.orderBy("id").collectAsList();
      assertEquals(50_001, actual.size());
      assertNestedArray(actual.get(0), 0);
      assertNestedArray(actual.get(49_999), 49_999);
      assertNestedArray(actual.get(50_000), 50_000);
    } finally {
      spark.conf().set(confKey, previousValue);
    }
  }

  @Test
  void readsPartitionValuesInRowMode() {
    final StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("part", DataTypes.StringType, true);
    final List<Row> sourceRows = Arrays.asList(
        RowFactory.create(1, "east"), RowFactory.create(2, "west"));
    final String path = tempDir.resolve("partitioned-data").toString();
    spark.createDataFrame(sourceRows, schema).write().mode(SaveMode.Overwrite)
        .partitionBy("part").format(FORMAT).save(path);

    final String confKey = "spark.sql.codegen.wholeStage";
    final String previousValue = spark.conf().get(confKey);
    try {
      spark.conf().set(confKey, "false");
      final Dataset<Row> read = spark.read().format(FORMAT).schema(schema).load(path);
      assertTrue(read.queryExecution().executedPlan().toString().contains("Batched: false"));
      assertEquals(sourceRows, read.orderBy("id").collectAsList());
    } finally {
      spark.conf().set(confKey, previousValue);
    }
  }

  private void assertNestedArray(final Row row, final int expectedId) {
    assertEquals(expectedId, row.getLong(0));
    assertEquals(Arrays.asList(expectedId, expectedId + 1), row.getStruct(1).getList(0));
  }

  private void assertRowModeRead(final String confKey, final String confValue) {
    final StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true);
    final List<Row> sourceRows = Arrays.asList(
        RowFactory.create(1, "one"),
        RowFactory.create(2, null),
        RowFactory.create(3, "three"));
    final String path = tempDir.resolve("data").toString();
    spark.createDataFrame(sourceRows, schema).coalesce(1).write()
        .mode(SaveMode.Overwrite).format(FORMAT).save(path);

    final String previousValue = spark.conf().get(confKey);
    try {
      spark.conf().set(confKey, confValue);
      final Dataset<Row> read = spark.read().format(FORMAT).schema(schema).load(path);
      assertTrue(read.queryExecution().executedPlan().toString().contains("Batched: false"));

      final List<Row> actual = read.orderBy("id").collectAsList();
      assertEquals(sourceRows, actual);
    } finally {
      spark.conf().set(confKey, previousValue);
    }
  }
}
