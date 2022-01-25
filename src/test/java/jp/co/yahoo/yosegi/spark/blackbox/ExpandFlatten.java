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

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ExpandFlatten {
  private static SparkSession spark;
  private static SQLContext sqlContext;
  private static final String appName = "ExpandFlattenTest";

  public boolean deleteDirectory(final File directory) {
    final File[] allContents = directory.listFiles();
    if (allContents != null) {
      for (final File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directory.delete();
  }

  public String getResourcePath(final String resource) {
    return Thread.currentThread().getContextClassLoader().getResource(resource).getPath();
  }

  public String getTmpPath() {
    return System.getProperty("java.io.tmpdir") + appName + ".yosegi";
  }

  public Dataset<Row> loadJsonFile(final String resource, final StructType schema) {
    final String resourcePath = getResourcePath(resource);
    if (schema == null) {
      return sqlContext.read().json(resourcePath).orderBy(col("id").asc());
    }
    return sqlContext.read().schema(schema).json(resourcePath).orderBy(col("id").asc());
  }

  public void createYosegiFile(final String resource) {
    final Dataset<Row> df = loadJsonFile(resource, null);
    final String tmpPath = getTmpPath();
    df.write()
        .mode(SaveMode.Overwrite)
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .save(tmpPath);
  }

  public Dataset<Row> loadYosegiFile(final Map<String, String> options, final StructType schema) {
    final String tmpPath = getTmpPath();
    DataFrameReader reader = sqlContext.read().format("jp.co.yahoo.yosegi.spark.YosegiFileFormat");
    if (schema != null) {
      reader = reader.schema(schema);
    }
    if (options != null) {
      reader = reader.options(options);
    }
    return reader.load(tmpPath).orderBy(col("id").asc());
  }

  @BeforeAll
  static void initAll() {
    spark = SparkSession.builder().appName(appName).master("local[*]").getOrCreate();
    sqlContext = spark.sqlContext();
  }

  @AfterAll
  static void tearDownAll() {
    spark.close();
  }

  @AfterEach
  void tearDown() {
    deleteDirectory(new File(getTmpPath()));
  }

  @Test
  void T_load_Expand_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Expand_1.txt";
    createYosegiFile(resource);

    // NOTE: expand options
    final Map<String, String> options =
        new HashMap<String, String>() {
          {
            put(
                "spread.reader.expand.column",
                "{\"base\":{\"node\":\"a1\", \"link_name\":\"ea1\"}}");
          }
        };
    // NOTE: load
    final Dataset<Row> dfj =
        loadJsonFile(resource, null)
            .withColumn("ea1", explode(col("a1")))
            .orderBy(col("id").asc(), col("ea1").asc());
    final Dataset<Row> dfy =
        loadYosegiFile(options, null).orderBy(col("id").asc(), col("ea1").asc());

    dfj.printSchema();
    dfj.show(false);
    dfy.printSchema();
    dfy.show(false);

    // NOTE: assert
    final List<Row> lrj = dfj.collectAsList();
    final List<Row> lry = dfy.collectAsList();
    final String[] fields = new String[] {"id", "a2", "ea1"};
    for (int i = 0; i < lrj.size(); i++) {
      for (final String field : fields) {
        assertEquals((Object) lrj.get(i).getAs(field), (Object) lry.get(i).getAs(field));
      }
    }
  }

  /*
   * FIXME: ArrayIndexOutOfBoundsException occurs if link size is larger than base size.
   *  * {"id":4, "a1":[10,11,12], "a2":[10.10,11.11,12.12,13.13]}
   * FIXME: The order would change when base and link are different sizes.
   *  * {"id":1, "a1":[], "a2": [0.0]}
   *  * {"id":3, "a1":[7,8,9], "a2":[7.7,8.8]}
   *   -> "ea1":[7,8,9], "ea2":[0.0,7.7,8,8]
   */
  @Test
  void T_load_Expand_2() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Expand_2.txt";
    createYosegiFile(resource);

    // NOTE: expand options
    final Map<String, String> options =
        new HashMap<String, String>() {
          {
            put(
                "spread.reader.expand.column",
                "{\"base\":{\"node\":\"a1\", \"link_name\":\"ea1\"}, \"parallel\":[{\"link_name\":\"ea2\", \"base_link_name\":\"ea1\", \"nodes\":[\"a2\"]}]}");
          }
        };
    // NOTE: load
    final Dataset<Row> dfj =
        loadJsonFile(resource, null)
            .withColumn("ea1", explode(col("a1")))
            .orderBy(col("id").asc(), col("ea1").asc());
    final Dataset<Row> dfy =
        loadYosegiFile(options, null).orderBy(col("id").asc(), col("ea1").asc());

    dfj.printSchema();
    dfj.show(false);
    dfy.printSchema();
    dfy.show(false);

    // NOTE: assert
    final List<Row> lrj = dfj.collectAsList();
    final List<Row> lry = dfy.collectAsList();
    final String[] fields = new String[] {"id", "ea1"};
    int cnte = 0;
    Object preId = null;
    for (int i = 0; i < lrj.size(); i++) {
      for (final String field : fields) {
        assertEquals((Object) lrj.get(i).getAs(field), (Object) lry.get(i).getAs(field));
      }
      if (preId == null || !preId.equals(lrj.get(i).getAs("id"))) {
        cnte = 0;
      }
      final List<Double> a2j = lrj.get(i).getList(lrj.get(i).fieldIndex("a2"));
      final Double ea2 = lry.get(i).getAs("ea2");
      if (cnte >= a2j.size()) {
        assertNull(ea2);
      } else {
        assertEquals(a2j.get(cnte), ea2);
      }
      preId = lrj.get(i).getAs("id");
      cnte++;
    }
  }

  @Test
  void T_load_Flatten_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Flatten_1.txt";
    createYosegiFile(resource);

    // NOTE: flatten options
    final Map<String, String> options =
        new HashMap<String, String>() {
          {
            put(
                "spread.reader.flatten.column",
                "[{\"link_name\":\"id\", \"nodes\":[\"id\"]}, {\"link_name\":\"m1k1\", \"nodes\":[\"m1\",\"k1\"]}, {\"link_name\":\"m1k2\", \"nodes\":[\"m1\",\"k2\"]}]");
          }
        };
    // NOTE: schema
    final DataType mapType =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true);
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("m1", mapType, true)));
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(options, null);

    dfj.printSchema();
    dfj.show(false);
    dfy.printSchema();
    dfy.show(false);

    // NOTE: assert
    final Map<String, String> fkeys =
        new HashMap<String, String>() {
          {
            put("k1", "m1k1");
            put("k2", "m1k2");
          }
        };
    final List<Row> lrj = dfj.collectAsList();
    final List<Row> lry = dfy.collectAsList();
    for (int i = 0; i < lrj.size(); i++) {
      final scala.collection.Map<String, String> mj = lrj.get(i).getMap(lrj.get(i).fieldIndex("m1"));
      for (final Map.Entry<String, String> entry : fkeys.entrySet()) {
        final String orgKey = entry.getKey();
        final String flatKey = entry.getValue();
        if (mj.contains(orgKey)) {
          assertEquals(mj.get(orgKey).get(), lry.get(i).getAs(flatKey));
        } else {
          assertNull(lry.get(i).getAs(flatKey));
        }
      }
    }
  }
}
