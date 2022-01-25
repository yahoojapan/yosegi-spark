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
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Load {
  private static SparkSession spark;
  private static SQLContext sqlContext;
  private static String appName = "LoadTest";

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

  public Dataset<Row> loadYosegiFile(final StructType schema) {
    final String tmpPath = getTmpPath();
    if (schema == null) {
      return sqlContext
          .read()
          .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
          .load(tmpPath)
          .orderBy(col("id").asc());
    }
    return sqlContext
        .read()
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .schema(schema)
        .load(tmpPath)
        .orderBy(col("id").asc());
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
  void T_load_Primitive_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Primitive_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("de", DataTypes.createDecimalType(precision, scale), true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final StructType structType = DataTypes.createStructType(fields);
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      for (final StructField field : fields) {
        final String name = field.name();
        final DataType dataType = field.dataType();
        assertEquals((Object) ldfj.get(i).getAs(name), (Object) ldfy.get(i).getAs(name));
      }
    }
  }

  @Test
  void T_load_Array_Integer_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Array_Integer_1.txt";
    createYosegiFile(resource);

    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, null);
    final Dataset<Row> dfy = loadYosegiFile(null);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      final int jIndex = ldfj.get(i).fieldIndex("a");
      final int yIndex = ldfy.get(i).fieldIndex("a");
      if (ldfj.get(i).isNullAt(jIndex)) {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:null, yosegi:null
          assertTrue(ldfy.get(i).isNullAt(yIndex));
        } else {
          // NOTE: json:null, yosegi:[]
          assertEquals(0, ldfy.get(i).getList(yIndex).size());
        }
      } else {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:[], yosegi:null
          assertEquals(0, ldfj.get(i).getList(jIndex).size());
        } else {
          // NOTE: json:[], yosegi:[]
          final List<Object> lj = ldfj.get(i).getList(jIndex);
          final List<Object> ly = ldfy.get(i).getList(yIndex);
          assertArrayEquals(lj.toArray(), ly.toArray());
        }
      }
    }
  }

  @Test
  void T_load_Struct_Primitive_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Struct_Primitive_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("de", DataTypes.createDecimalType(precision, scale), true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("s", DataTypes.createStructType(fields), true)));
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      final int jIndex = ldfj.get(i).fieldIndex("s");
      final int yIndex = ldfy.get(i).fieldIndex("s");
      if (ldfj.get(i).isNullAt(jIndex)) {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:s:null, yosegi:s:null
          assertTrue(ldfy.get(i).isNullAt(yIndex));
        } else {
          // NOTE: json:s:null, yosegi:s.field:null
          final Row ry = ldfy.get(i).getStruct(yIndex);
          for (final StructField field : fields) {
            final String name = field.name();
            assertNull(ry.getAs(name));
          }
        }
      } else {
        // NOTE: json:s.field, yosegi:s.field
        final Row rj = ldfj.get(i).getStruct(jIndex);
        final Row ry = ldfy.get(i).getStruct(yIndex);
        for (final StructField field : fields) {
          final String name = field.name();
          assertEquals((Object) rj.getAs(name), (Object) ry.getAs(name));
        }
      }
    }
  }

  @Test
  void T_load_Map_Integer_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Map_Integer_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final DataType mapType =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true);
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("m", mapType, true)));
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      final int jIndex = ldfj.get(i).fieldIndex("m");
      final int yIndex = ldfy.get(i).fieldIndex("m");
      if (ldfj.get(i).isNullAt(jIndex)) {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:m:null, yosegi:m:null
          assertTrue(ldfy.get(i).isNullAt(yIndex));
        } else {
          // NOTE: json:m:null, yosegi:m:{}
          assertEquals(0, ldfy.get(i).getMap(yIndex).size());
        }
      } else {
        final Map<String, Integer> mj = ldfj.get(i).getMap(jIndex);
        final Map<String, Integer> my = ldfy.get(i).getMap(yIndex);
        final Iterator<String> iter = mj.keysIterator();
        while (iter.hasNext()) {
          final String key = iter.next();
          if (mj.get(key).get() == null) {
            // NOTE: json:m.key:null, yosegi:m.key:not exist
            assertTrue(my.get(key).isEmpty());
          } else {
            // NOTE: json:m.key, yosegi:m.key
            assertEquals(mj.get(key), my.get(key));
          }
        }
      }
    }
  }

  @Test
  void T_load_Array_Array_Integer_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Array_Array_Integer_1.txt";
    createYosegiFile(resource);

    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, null);
    final Dataset<Row> dfy = loadYosegiFile(null);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      final int jIndex = ldfj.get(i).fieldIndex("aa");
      final int yIndex = ldfy.get(i).fieldIndex("aa");
      if (ldfj.get(i).isNullAt(jIndex)) {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:null, yosegi:null
          assertTrue(ldfy.get(i).isNullAt(yIndex));
        } else {
          // FIXME: json:null, yosegi:[]
          assertTrue(false);
        }
      } else {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:[], yosegi:null
          assertEquals(0, ldfj.get(i).getList(jIndex).size());
        } else {
          final List<WrappedArray<Long>> ldfj2 = ldfj.get(i).getList(jIndex);
          final List<WrappedArray<Long>> ldfy2 = ldfy.get(i).getList(yIndex);
          for (int j = 0; j < ldfj2.size(); j++) {
            final WrappedArray<Long> waj = ldfj2.get(j);
            final WrappedArray<Long> way = ldfy2.get(j);
            if (way == null) {
              if (waj == null) {
                // NOTE: json:[null], yosegi:[null]
                assertNull(waj);
              } else {
                // NOTE: json:[[]], yosegi:[null]
                assertEquals(0, waj.size());
              }
            } else {
              // NOTE: json:[[]], yosegi:[[]]
              for (int k = 0; k < waj.size(); k++) {
                assertEquals(waj.apply(k), way.apply(k));
              }
            }
          }
        }
      }
    }
  }

  @Test
  void T_load_Array_Struct_Primitive_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Array_Struct_Primitive_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("de", DataTypes.createDecimalType(precision, scale), true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final DataType arrayType = DataTypes.createArrayType(DataTypes.createStructType(fields), true);
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("as", arrayType, true)));

    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      final int jIndex = ldfj.get(i).fieldIndex("as");
      final int yIndex = ldfy.get(i).fieldIndex("as");
      if (ldfj.get(i).isNullAt(jIndex)) {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:null, yosegi:null
          assertTrue(ldfy.get(i).isNullAt(yIndex));
        } else {
          // FIXME: json:null, yosegi:[]
          assertTrue(false);
        }
      } else {
        if (ldfy.get(i).isNullAt(yIndex)) {
          final List<Row> lrj = ldfj.get(i).getList(jIndex);
          for (int j = 0; j < lrj.size(); j++) {
            final Row rj = lrj.get(j);
            if (rj == null) {
              // NOTE: json:[null], yosegi:null
              assertNull(rj);
            } else {
              // NOTE: json[as.field:null], yosegi:null
              for (final StructField field : fields) {
                final String name = field.name();
                assertNull(rj.getAs(name));
              }
            }
          }
        } else {
          final List<Row> lrj = ldfj.get(i).getList(jIndex);
          final List<Row> lry = ldfy.get(i).getList(yIndex);
          for (int j = 0; j < lrj.size(); j++) {
            final Row rj = lrj.get(j);
            final Row ry = lry.get(j);
            if (ry == null) {
              if (rj == null) {
                // NOTE: json:[null], yosegi:[null]
                assertNull(rj);
              } else {
                // NOTE: json:[{}], yosegi:[null]
                for (final StructField field : fields) {
                  final String name = field.name();
                  assertNull(rj.getAs(name));
                }
              }
            } else {
              if (rj == null) {
                // NOTE: json:[null], yosegi:[{}]
                for (final StructField field : fields) {
                  final String name = field.name();
                  assertNull(ry.getAs(name));
                }
              } else {
                // NOTE: json:[{}], yosegi:[{}]
                for (final StructField field : fields) {
                  final String name = field.name();
                  assertEquals((Object) rj.getAs(name), (Object) ry.getAs(name));
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  void T_load_Array_Map_Integer_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Array_Map_Integer_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final DataType mapType =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true);
    final DataType arrayType = DataTypes.createArrayType(mapType, true);
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("am", arrayType, true)));
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      final int jIndex = ldfj.get(i).fieldIndex("am");
      final int yIndex = ldfy.get(i).fieldIndex("am");
      if (ldfj.get(i).isNullAt(jIndex)) {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:null, yosegi:null
          assertTrue(ldfy.get(i).isNullAt(yIndex));
        } else {
          // FIXME: json:null, yosegi:[]
          assertTrue(false);
        }
      } else {
        if (ldfy.get(i).isNullAt(yIndex)) {
          // NOTE: json:[], yosegi:null
          final List<Map<String, Integer>> lmj = ldfj.get(i).getList(jIndex);
          for (int j = 0; j < lmj.size(); j++) {
            final Map<String, Integer> mj = lmj.get(j);
            if (mj == null) {
              // NOTE: json:[null], yosegi:null
              assertNull(mj);
            } else {
              // NOTE: json:[am.key:null], yosegi:null
              final Iterator<String> iter = mj.keysIterator();
              while (iter.hasNext()) {
                final String key = iter.next();
                assertNull(mj.get(key).get());
              }
            }
          }
        } else {
          // NOTE: json:[], yosegi:[]
          final List<Map<String, Integer>> lmj = ldfj.get(i).getList(jIndex);
          final List<Map<String, Integer>> lmy = ldfy.get(i).getList(yIndex);
          for (int j = 0; j < lmj.size(); j++) {
            final Map<String, Integer> mj = lmj.get(j);
            final Map<String, Integer> my = lmy.get(j);
            if (my == null) {
              if (mj == null) {
                // NOTE: json:[null], yosegi:[null]
                assertNull(mj);
              } else {
                // NOTE: json:[{}], yosegi:[null]
                final Iterator<String> iter = mj.keysIterator();
                while (iter.hasNext()) {
                  final String key = iter.next();
                  assertNull(mj.get(key).get());
                }
              }
            } else {
              if (mj == null) {
                // NOTE: json:[null], yosegi:[{}]
                final Iterator<String> iter = my.keysIterator();
                while (iter.hasNext()) {
                  final String key = iter.next();
                  assertNull(my.get(key).get());
                }
              } else {
                // NOTE: json[{}], yosegi:[{}]
                final Iterator<String> iter = mj.keysIterator();
                while (iter.hasNext()) {
                  final String key = iter.next();
                  if (mj.get(key).get() == null) {
                    // NOTE: json:[{key:null}], yosegi:[{key:not exist}]
                    assertTrue(my.get(key).isEmpty());
                  } else {
                    // NOTE: json:[{key}], yosegi:[{key}]
                    assertEquals(mj.get(key), my.get(key));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /*
   * FIXME: NullPointerException occurs when writing yosegi file.
   *  * {"id":1}
   *  * {"id":2, "sa":null}
   */
  @Test
  void T_load_Struct_Array_Primitive_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Struct_Array_Primitive_1.txt";
    createYosegiFile(resource);

    // NOTE: scheme key
    final String[] fields = new String[] {"ai", "as"};
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, null);
    final Dataset<Row> dfy = loadYosegiFile(null);

    // NOTE: assert
    final List<Row> lrj = dfj.collectAsList();
    final List<Row> lry = dfy.collectAsList();
    for (int i = 0; i < lrj.size(); i++) {
      final int ji1 = lrj.get(i).fieldIndex("sa");
      final int yi1 = lry.get(i).fieldIndex("sa");
      if (lrj.get(i).isNullAt(ji1)) {
        // NOTE: json:sa:null or sa:not exist or sa:{empty}
        if (lry.get(i).isNullAt(yi1)) {
          // NOTE: json:sa:null or sa:not exist or sa:{empty}
          //       yosegi:sa:null or sa:not exist
          assertTrue(lry.get(i).isNullAt(yi1));
        } else {
          // NOTE: json:sa:null or sa:not exist or sa:{empty}
          //       yosegi:sa.field:null or sa.field:[] or sa.field:[null]
          final Row ry = lry.get(i).getStruct(yi1);
          for (final String name : fields) {
            final List<Object> loy = ry.getAs(name);
            if (loy == null) {
              // NOTE: yosegi:sa.field:null
              assertNull(loy);
            } else {
              if (loy.size() == 0) {
                // NOTE: yosegi:sa.field:[empty]
                assertEquals(0, loy.size());
              } else {
                // NOTE: yosegi:sa.field:[null]
                for (int j = 0; j < loy.size(); j++) {
                  assertNull(loy.get(j));
                }
              }
            }
          }
        }
      } else {
        // NOTE: json:sa:{}
        if (lry.get(i).isNullAt(yi1)) {
          // NOTE: json:sa:{}
          //       yosegi:sa:null or sa:not exist or sa:{empty}
          final Row rj = lrj.get(i).getStruct(ji1);
          for (final String name : fields) {
            final List<Object> loj = rj.getAs(name);
            if (loj == null) {
              // NOTE: json:sa.field:null
              assertNull(null);
            } else {
              if (loj.size() == 0) {
                // NOTE: json:sa.field:[empty]
                assertEquals(0, loj.size());
              } else {
                // NOTE: json:sa.field:[null]
                for (int j = 0; j < loj.size(); j++) {
                  assertNull(loj.get(j));
                }
              }
            }
          }
        } else {
          // NOTE: json:sa:{}, yosegi:sa:{}
          final Row rj = lrj.get(i).getStruct(ji1);
          final Row ry = lry.get(i).getStruct(yi1);
          for (final String name : fields) {
            final WrappedArray<Object> waj = rj.getAs(name);
            final WrappedArray<Object> way = ry.getAs(name);
            if (waj == null) {
              // NOTE: json:sa.field:null
              if (way == null) {
                // NOTE: yosegi:sa.field:null
                assertNull(way);
              } else {
                if (way.size() == 0) {
                  // NOTE: yosegi:sa.field:[empty]
                  assertEquals(0, way.size());
                } else {
                  // NOTE: yosegi:sa.field:[null]
                  for (int j = 0; j < way.size(); j++) {
                    assertNull(way.apply(j));
                  }
                }
              }
            } else {
              // NOTE: json:sa.field
              if (way == null) {
                // NOTE: yosegi:sa.field:null
                if (waj.size() == 0) {
                  // NOTE: json:sa.field:[empty]
                  assertEquals(0, waj.size());
                } else {
                  // NOTE: json:sa.field:[null]
                  for (int j = 0; j < waj.size(); j++) {
                    assertNull(waj.apply(j));
                  }
                }
              } else {
                // NOTE: yosegi:sa.field
                if (waj.size() == 0) {
                  // NOTE: json:sa.field:[empty]
                  if (way.size() == 0) {
                    // NOTE: yosegi:sa.field:[empty]
                    assertEquals(0, way.size());
                  } else {
                    // NOTE: yosegi:sa.field:[null]
                    for (int j = 0; j < way.size(); j++) {
                      assertNull(way.apply(j));
                    }
                  }
                } else {
                  // NOTE: json:sa.field:[]
                  for (int j = 0; j < waj.size(); j++) {
                    assertEquals(waj.apply(j), way.apply(j));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /*
   * FIXME: NullPointerException occurs when writing yosegi file.
   *  * {"id":1}
   *  * {"id":2, "ss":null}
   */
  @Test
  void T_load_Struct_Struct_Primitive_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Struct_Struct_Primitive_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("de", DataTypes.createDecimalType(precision, scale), true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final StructType structType2 =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("s1", DataTypes.createStructType(fields), true),
                DataTypes.createStructField("s2", DataTypes.createStructType(fields), true)));
    final String[] fields1 = new String[] {"s1", "s2"};
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("ss", structType2, true)));
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> lrj = dfj.collectAsList();
    final List<Row> lry = dfy.collectAsList();
    for (int i = 0; i < lrj.size(); i++) {
      final int ji1 = lrj.get(i).fieldIndex("ss");
      final int yi1 = lry.get(i).fieldIndex("ss");
      if (lrj.get(i).isNullAt(ji1)) {
        // NOTE: json:ss:null or ss:not exist
        if (lry.get(i).isNullAt(yi1)) {
          // NOTE: yosegi:ss:null or ss:not exist
          assertTrue(lry.get(i).isNullAt(yi1));
        } else {
          // NOTE: yosegi:ss:{empty} or ss.field:null or ss.field:{empty} or ss.field.field:null
          final Row ry1 = lry.get(i).getStruct(yi1);
          for (final String name : fields1) {
            final Row ry2 = ry1.getAs(name);
            if (ry2 == null) {
              // NOTE: yosegi:ss.field:null
              assertNull(ry2);
            } else if (ry2.size() == 0) {
              // NOTE: yosegi:ss.field:{empty}
              assertEquals(0, ry2.size());
            } else {
              // NOTE: yosegi:ss.field.field:null
              for (final StructField field : fields) {
                assertNull(ry2.getAs(field.name()));
              }
            }
          }
        }
      } else {
        // NOTE: json:ss:{}
        if (lry.get(i).isNullAt(yi1)) {
          // NOTE: yosegi:ss:null or ss:not exist
          final Row rj1 = lrj.get(i).getStruct(ji1);
          for (final String name : fields1) {
            final Row rj2 = rj1.getAs(name);
            if (rj2 == null) {
              // NOTE: json:ss.field:null
              assertNull(rj2);
            } else if (rj2.size() == 0) {
              // NOTE: json:ss.field:{empty}
              assertEquals(0, rj2.size());
            } else {
              // NOTE: json:ss.field.field:null
              for (final StructField field : fields) {
                assertNull(rj2.getAs(field.name()));
              }
            }
          }
        } else {
          // NOTE: yosegi:ss:{}
          final Row rj1 = lrj.get(i).getStruct(ji1);
          final Row ry1 = lry.get(i).getStruct(yi1);
          for (final String name : fields1) {
            final Row rj2 = rj1.getAs(name);
            final Row ry2 = ry1.getAs(name);
            if (rj2 == null) {
              // NOTE: json:ss.field:null
              if (ry2 == null) {
                // NOTE: yosegi:ss.field:null
                assertNull(ry2);
              } else if (ry2.size() == 0) {
                // NOTE: yosegi:ss.field:{empty}
                assertEquals(0, ry2.size());
              } else {
                // NOTE: yosegi:ss.field.field:null
                for (final StructField field : fields) {
                  assertNull(ry2.getAs(field.name()));
                }
              }
            } else {
              // NOTE: json:ss.field:{}
              if (ry2 == null) {
                // NOTE: yosegi:ss.field:null
                if (rj2.size() == 0) {
                  // NOTE: json:ss.field:{empty}
                  assertEquals(0, rj2.size());
                } else {
                  // NOTE: json:ss.field.field:null
                  for (final StructField field : fields) {
                    assertNull(rj2.getAs(field.name()));
                  }
                }
              } else {
                // NOTE: yosegi:ss.field:{}
                if (rj2.size() == 0) {
                  // NOTE: json:ss.field:{empty}
                  if (ry2.size() == 0) {
                    // NOTE: yosegi:ss.field:{empty}
                    assertEquals(0, ry2.size());
                  } else {
                    // NOTE: yosegi:ss.field.field:null
                    for (final StructField field : fields) {
                      assertNull(ry2.getAs(field.name()));
                    }
                  }
                } else {
                  // NOTE: json:ss.field:{}
                  for (final StructField field : fields) {
                    assertEquals(
                        (Object) rj2.getAs(field.name()), (Object) ry2.getAs(field.name()));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /*
   * FIXME: NullPointerException occurs when writing yosegi file.
   *  * {"id":1}
   *  * {"id":2, "sm":null}
   */
  @Test
  void T_load_Struct_Map_Integer_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Struct_Map_Integer_1.txt";
    createYosegiFile(resource);

    // NOTE: schema
    final DataType mapType =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true);
    final StructType structType2 =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("mi1", mapType, true),
                DataTypes.createStructField("mi2", mapType, true)));
    final String[] fields1 = new String[] {"mi1", "mi2"};
    final StructType structType =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("sm", structType2, true)));
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> lrj = dfj.collectAsList();
    final List<Row> lry = dfy.collectAsList();
    for (int i = 0; i < lrj.size(); i++) {
      final int ji1 = lrj.get(i).fieldIndex("sm");
      final int yi1 = lry.get(i).fieldIndex("sm");
      if (lrj.get(i).isNullAt(ji1)) {
        // NOTE: json:sm:null or sm:not exist
        if (lry.get(i).isNullAt(yi1)) {
          // NOTE: yosegi:sm:null or sm:not exist
          assertTrue(lry.get(i).isNullAt(yi1));
        } else if (lry.get(i).size() == 0) {
          // NOTE: yosegi:sm:{empty}
          assertEquals(0, lry.get(i).size());
        } else {
          // NOTE: yosegi:sm.field:null or sm.field:{empty} or sm.field.key:null
          final Row ry = lry.get(i).getStruct(yi1);
          for (final String name : fields1) {
            final Map<String, Integer> my = ry.getAs(name);
            if (my == null) {
              // NOTE: yosegi:sm.field:null
              assertNull(my);
            } else if (my.size() == 0) {
              // NOTE: yosegi:sm.field:{empty}
              assertEquals(0, my.size());
            } else {
              // NOTE: yosegi:sm.field.key:null
              final Iterator<String> iter = my.keysIterator();
              while (iter.hasNext()) {
                final String key = iter.next();
                assertNull(my.get(key));
              }
            }
          }
        }
      } else {
        // NOTE: json:sm:{}
        if (lry.get(i).isNullAt(yi1)) {
          // NOTE: yosegi:sm:null or sm:not exist
          final Row rj = lrj.get(i).getStruct(ji1);
          for (final String name : fields1) {
            final Map<String, Integer> mj = rj.getAs(name);
            if (mj == null) {
              // NOTE: json:sm.field:null
              assertNull(mj);
            } else if (mj.size() == 0) {
              // NOTE: json:sm.field:{empty}
              assertEquals(0, mj.size());
            } else {
              // NOTE: json:sm.field.key:null
              final Iterator<String> iter = mj.keysIterator();
              while (iter.hasNext()) {
                final String key = iter.next();
                assertNull(mj.get(key));
              }
            }
          }
        } else {
          // NOTE: yosegi:sm:{}
          final Row rj = lrj.get(i).getStruct(ji1);
          final Row ry = lry.get(i).getStruct(yi1);
          for (final String name : fields1) {
            final Map<String, Integer> mj = rj.getAs(name);
            final Map<String, Integer> my = ry.getAs(name);
            if (mj == null) {
              // NOTE: json:sm.field:null
              if (my == null) {
                // NOTE: yosegi:sm.field:null
                assertNull(my);
              } else if (my.size() == 0) {
                // NOTE: yosegi:sm.field:{empty}
                assertEquals(0, my.size());
              } else {
                // NOTE: yosegi:sm.field.key:null
                final Iterator<String> iter = my.keysIterator();
                while (iter.hasNext()) {
                  final String key = iter.next();
                  assertNull(my.get(key));
                }
              }
            } else {
              // NOTE: json:sm.field:{}
              if (my == null) {
                // NOTE: yosegi:sm.field:null
                if (mj.size() == 0) {
                  // NOTE: json:sm.field:{empty}
                  assertEquals(0, mj.size());
                } else {
                  // NOTE: json:sm.field.key:null
                  final Iterator<String> iter = mj.keysIterator();
                  while (iter.hasNext()) {
                    final String key = iter.next();
                    assertNull(mj.get(key));
                  }
                }
              } else {
                // NOTE: yosegi:sm.field:{}
                if (mj.size() == 0) {
                  // NOTE: json:sm.field:{empty}
                  if (my.size() == 0) {
                    // NOTE: yosegi:sm.field:{empty}
                    assertEquals(0, my.size());
                  } else {
                    // NOTE: yosegi:sm.field.key:null
                    final Iterator<String> iter = my.keysIterator();
                    while (iter.hasNext()) {
                      final String key = iter.next();
                      assertNull(my.get(key));
                    }
                  }
                } else {
                  // NOTE: json:sm.field:{}
                  final Iterator<String> iter = mj.keysIterator();
                  while (iter.hasNext()) {
                    final String key = iter.next();
                    assertEquals(mj.get(key), mj.get(key));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
