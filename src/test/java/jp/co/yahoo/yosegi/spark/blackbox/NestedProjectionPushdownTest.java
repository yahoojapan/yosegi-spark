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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NestedProjectionPushdownTest {

  private static SparkSession spark;
  private static final String appName = "NestedProjectionPushdownTest";

  public boolean deleteDirectory(final File directory) {
    final File[] allContents = directory.listFiles();
    if (allContents != null) {
      for (final File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directory.delete();
  }

  public String getTmpPath(final String subDir) {
    String tmpdir = System.getProperty("java.io.tmpdir");
    if (tmpdir.endsWith("/")) {
      tmpdir = tmpdir.substring(0, tmpdir.length() - 1);
    }
    return tmpdir + "/" + appName + "/" + subDir;
  }

  @BeforeAll
  static void initAll() {
    spark = SparkSession.builder()
        .appName(appName)
        .master("local[*]")
        .config("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
        .getOrCreate();
  }

  @AfterAll
  static void tearDownAll() {
    if (spark != null) {
      spark.close();
    }
  }

  @AfterEach
  void tearDown() {
    deleteDirectory(new File(getTmpPath("")));
  }

  @Test
  void testNestedStructFieldPruning() {
    final String path = getTmpPath("testNestedStructFieldPruning");

    // Schema: id, parent_a: struct<f1: long, f2: string, f3: string>, parent_b: struct<f4: string, f5: int>
    StructType parentASchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("f1", DataTypes.LongType, true),
        DataTypes.createStructField("f2", DataTypes.StringType, true),
        DataTypes.createStructField("f3", DataTypes.StringType, true)
    ));
    StructType parentBSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("f4", DataTypes.StringType, true),
        DataTypes.createStructField("f5", DataTypes.IntegerType, true)
    ));
    StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("parent_a", parentASchema, true),
        DataTypes.createStructField("parent_b", parentBSchema, true)
    ));

    List<Row> data = Arrays.asList(
        RowFactory.create(1, RowFactory.create(1000L, "val_a_1", "meta_1"), RowFactory.create("val_b_1", 25)),
        RowFactory.create(2, RowFactory.create(2000L, "val_a_2", "meta_2"), RowFactory.create("val_b_2", 30)),
        RowFactory.create(3, null, RowFactory.create("val_b_3", 35)),
        RowFactory.create(4, RowFactory.create(4000L, "val_a_4", "meta_4"), null)
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);
    df.write().mode(SaveMode.Overwrite).format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").save(path);

    Dataset<Row> yosegiDf = spark.read().format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").schema(schema).load(path);

    // Select nested columns: parent_a.f2 and parent_b.f4 (nested pruning active)
    Dataset<Row> selected = yosegiDf.select(col("id"), col("parent_a.f2"), col("parent_b.f4")).orderBy(col("id").asc());
    List<Row> rows = selected.collectAsList();

    assertEquals(4, rows.size());

    // Row 1
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("val_a_1", rows.get(0).getString(1));
    assertEquals("val_b_1", rows.get(0).getString(2));

    // Row 2
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("val_a_2", rows.get(1).getString(1));
    assertEquals("val_b_2", rows.get(1).getString(2));

    // Row 3 (parent_a is null)
    assertEquals(3, rows.get(2).getInt(0));
    assertNull(rows.get(2).get(1));
    assertEquals("val_b_3", rows.get(2).getString(2));

    // Row 4 (parent_b is null)
    assertEquals(4, rows.get(3).getInt(0));
    assertEquals("val_a_4", rows.get(3).getString(1));
    assertNull(rows.get(3).get(2));
  }

  @Test
  void testDeeplyNestedStruct() {
    final String path = getTmpPath("testDeeplyNestedStruct");

    // a.b.c: int, a.b.d: string, a.e: string
    StructType level3 = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("c", DataTypes.IntegerType, true),
        DataTypes.createStructField("d", DataTypes.StringType, true)
    ));
    StructType level2 = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("b", level3, true),
        DataTypes.createStructField("e", DataTypes.StringType, true)
    ));
    StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("a", level2, true)
    ));

    List<Row> data = Arrays.asList(
        RowFactory.create(1, RowFactory.create(RowFactory.create(100, "d_val_1"), "e_val_1")),
        RowFactory.create(2, RowFactory.create(RowFactory.create(200, "d_val_2"), "e_val_2"))
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);
    df.write().mode(SaveMode.Overwrite).format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").save(path);

    Dataset<Row> yosegiDf = spark.read().format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").schema(schema).load(path);

    // Prune to only a.b.c
    Dataset<Row> selected = yosegiDf.select(col("id"), col("a.b.c")).orderBy(col("id").asc());
    List<Row> rows = selected.collectAsList();

    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(100, rows.get(0).getInt(1));

    assertEquals(2, rows.get(1).getInt(0));
    assertEquals(200, rows.get(1).getInt(1));
  }

  @Test
  void testStructWithArrayAndMapCorrectness() {
    final String path = getTmpPath("testStructWithArrayAndMapCorrectness");

    // Struct containing array, map, and primitive fields
    StructType sSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("arr", DataTypes.createArrayType(DataTypes.StringType), true),
        DataTypes.createStructField("map", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true),
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("unused", DataTypes.StringType, true)
    ));
    StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("s", sSchema, true)
    ));

    Map<String, Integer> map1 = new HashMap<>();
    map1.put("k1", 10);
    map1.put("k2", 20);

    List<Row> data = Arrays.asList(
        RowFactory.create(1, RowFactory.create(Arrays.asList("x", "y"), map1, "name_1", "unused_1")),
        RowFactory.create(2, RowFactory.create(Collections.emptyList(), Collections.emptyMap(), "name_2", "unused_2")),
        RowFactory.create(3, null)
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);
    df.write().mode(SaveMode.Overwrite).format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").save(path);

    Dataset<Row> yosegiDf = spark.read().format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").schema(schema).load(path);

    // Select s.arr, s.map, s.name (pruning out s.unused)
    Dataset<Row> selected = yosegiDf.select(col("id"), col("s.name"), col("s.arr"), col("s.map")).orderBy(col("id").asc());
    List<Row> rows = selected.collectAsList();

    assertEquals(3, rows.size());

    // Row 1
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("name_1", rows.get(0).getString(1));
    List<String> arr1 = rows.get(0).getList(2);
    assertEquals(Arrays.asList("x", "y"), arr1);
    scala.collection.Map<String, Integer> m1 = rows.get(0).getMap(3);
    assertEquals(10, (int) m1.get("k1").get());
    assertEquals(20, (int) m1.get("k2").get());

    // Row 2
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("name_2", rows.get(1).getString(1));
    List<String> arr2 = rows.get(1).getList(2);
    assertEquals(0, arr2.size());

    // Row 3 (null struct)
    assertEquals(3, rows.get(2).getInt(0));
    assertNull(rows.get(2).get(1));
    assertTrue(rows.get(2).isNullAt(2) || rows.get(2).getList(2).isEmpty());
    assertTrue(rows.get(2).isNullAt(3) || rows.get(2).getMap(3).isEmpty());
  }

  @Test
  void testArrayOfStructAndMapOfPrimitives() {
    final String path = getTmpPath("testArrayOfStructAndMapOfPrimitives");

    StructType elemSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("elem_k", DataTypes.StringType, true),
        DataTypes.createStructField("elem_v", DataTypes.IntegerType, true)
    ));
    StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("arr_struct", DataTypes.createArrayType(elemSchema), true),
        DataTypes.createStructField("map_prim", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true)
    ));

    Map<String, Integer> mapData = new HashMap<>();
    mapData.put("entry1", 100);

    List<Row> data = Arrays.asList(
        RowFactory.create(
            1,
            Arrays.asList(RowFactory.create("a", 1), RowFactory.create("b", 2)),
            mapData
        )
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);
    df.write().mode(SaveMode.Overwrite).format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").save(path);

    Dataset<Row> yosegiDf = spark.read().format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").schema(schema).load(path);

    // Prune to only arr_struct
    Dataset<Row> selectedArr = yosegiDf.select(col("id"), col("arr_struct")).orderBy(col("id").asc());
    List<Row> rows = selectedArr.collectAsList();
    assertEquals(1, rows.size());
    List<Row> elemList = rows.get(0).getList(1);
    assertEquals(2, elemList.size());
    assertEquals("a", elemList.get(0).getString(0));
    assertEquals(1, elemList.get(0).getInt(1));
    assertEquals("b", elemList.get(1).getString(0));
    assertEquals(2, elemList.get(1).getInt(1));

    // Prune to only map_prim
    Dataset<Row> selectedMap = yosegiDf.select(col("id"), col("map_prim")).orderBy(col("id").asc());
    List<Row> mapRows = selectedMap.collectAsList();
    assertEquals(1, mapRows.size());
    scala.collection.Map<String, Integer> returnedMap = mapRows.get(0).getMap(1);
    assertEquals(100, (int) returnedMap.get("entry1").get());
  }

  @Test
  void testParityBetweenFullAndPrunedReads() {
    final String path = getTmpPath("testParityBetweenFullAndPrunedReads");

    StructType addressSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("city", DataTypes.StringType, true),
        DataTypes.createStructField("zip", DataTypes.StringType, true)
    ));
    StructType userSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true),
        DataTypes.createStructField("address", addressSchema, true),
        DataTypes.createStructField("tags", DataTypes.createArrayType(DataTypes.StringType), true)
    ));
    StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("user", userSchema, true),
        DataTypes.createStructField("extra", DataTypes.StringType, true)
    ));

    List<Row> data = Arrays.asList(
        RowFactory.create(1, RowFactory.create("Alice", 30, RowFactory.create("Tokyo", "100-0001"), Arrays.asList("admin", "dev")), "extra1"),
        RowFactory.create(2, RowFactory.create("Bob", 25, RowFactory.create("Osaka", "530-0001"), Collections.emptyList()), "extra2"),
        RowFactory.create(3, RowFactory.create("Charlie", 40, null, Arrays.asList("guest")), "extra3"),
        RowFactory.create(4, null, "extra4")
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);
    df.write().mode(SaveMode.Overwrite).format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").save(path);

    Dataset<Row> yosegiDf = spark.read().format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").schema(schema).load(path);

    // Read pruned columns: user.name, user.address.city, user.tags
    Dataset<Row> pruned = yosegiDf.select(col("id"), col("user.name"), col("user.address.city"), col("user.tags")).orderBy(col("id").asc());
    Dataset<Row> fullDfSelected = df.select(col("id"), col("user.name"), col("user.address.city"), col("user.tags")).orderBy(col("id").asc());

    List<Row> prunedRows = pruned.collectAsList();
    List<Row> expectedRows = fullDfSelected.collectAsList();

    assertEquals(expectedRows.size(), prunedRows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      Row exp = expectedRows.get(i);
      Row actual = prunedRows.get(i);
      assertEquals(exp.getInt(0), actual.getInt(0));
      assertEquals((Object) exp.getAs(1), (Object) actual.getAs(1));
      assertEquals((Object) exp.getAs(2), (Object) actual.getAs(2));
      if (exp.isNullAt(3)) {
        assertTrue(actual.isNullAt(3) || actual.getList(3).isEmpty());
      } else {
        assertEquals((Object) exp.getList(3), (Object) actual.getList(3));
      }
    }
  }

}
