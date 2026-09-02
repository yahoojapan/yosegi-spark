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

/**
 * Regression test for the nested projection pushdown fix: measures how many bytes the connector
 * actually reads from disk for a nested-pruned schema, versus the full file size. Before the fix,
 * {@code ProjectionPushdownUtil} only pushed top-level field names, so a schema pruned to a few
 * nested leaves still decoded the entire enclosing struct — pruned reads landed close to 100% of
 * the file. This test asserts the pruned read stays well below that (see the final assertion),
 * so a future regression that drops nested paths back to top-level-only pushdown fails loudly here
 * instead of only showing up as a production I/O regression.
 */
package jp.co.yahoo.yosegi.spark.blackbox;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PrunedReadBytesBenchTest {

  private static SparkSession spark;

  @BeforeAll
  static void up() {
    spark = SparkSession.builder().appName("bytes-bench").master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
  }

  @AfterAll
  static void down() {
    if (spark != null) {
      spark.close();
    }
  }

  private static long bytesRead() {
    long total = 0;
    for (FileSystem.Statistics s : FileSystem.getAllStatistics()) {
      if ("file".equals(s.getScheme())) {
        total += s.getBytesRead();
      }
    }
    return total;
  }

  @Test
  void measure() {
    String path = System.getProperty("java.io.tmpdir") + "/yosegi-bytes-bench";
    int n = 60000;
    Random r = new Random(42);
    List<Row> rows = new ArrayList<>(n);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      String[] h = new String[6];
      for (int k = 0; k < 6; k++) {
        sb.setLength(0);
        for (int c = 0; c < 200; c++) {
          sb.append((char) ('a' + r.nextInt(26)));
        }
        h[k] = sb.toString();
      }
      rows.add(RowFactory.create(
          RowFactory.create(h[0], h[1], h[2], h[3], h[4], h[5]),
          RowFactory.create((long) i, "id-" + i)));
    }
    StructType heavy = new StructType()
        .add("h1", DataTypes.StringType).add("h2", DataTypes.StringType)
        .add("h3", DataTypes.StringType).add("h4", DataTypes.StringType)
        .add("h5", DataTypes.StringType).add("h6", DataTypes.StringType);
    StructType light = new StructType()
        .add("x", DataTypes.LongType).add("y", DataTypes.StringType);
    StructType full = new StructType().add("heavy", heavy).add("light", light);

    Dataset<Row> df = spark.createDataFrame(rows, full).repartition(1);
    df.write().mode("overwrite").format("yosegi").save(path);

    long fileBytes = 0;
    for (File f : new File(path).listFiles()) {
      if (!f.getName().startsWith(".") && !f.getName().startsWith("_")) {
        fileBytes += f.length();
      }
    }

    // NOTE: count() empties requiredSchema (zero-column scan reads only block metadata),
    // so every measurement below must actually CONSUME column values.

    // (1) full read — sanity: should be close to fileBytes
    FileSystem.clearStatistics();
    long f0 = bytesRead();
    Row fullRow = spark.read().format("yosegi").schema(full).load(path)
        .selectExpr("sum(length(heavy.h1)+length(heavy.h2)+length(heavy.h3)"
            + "+length(heavy.h4)+length(heavy.h5)+length(heavy.h6)+length(light.y))",
            "sum(light.x)", "count(1)")
        .first();
    long fullBytes = bytesRead() - f0;
    long cntFull = fullRow.getLong(2);

    // (2) pruned to the light struct only — the production shape
    StructType prunedLight = new StructType().add("light", light);
    FileSystem.clearStatistics();
    long p0 = bytesRead();
    Row plRow = spark.read().format("yosegi").schema(prunedLight).load(path)
        .selectExpr("sum(light.x)", "sum(length(light.y))", "count(1)").first();
    long prunedBytes = bytesRead() - p0;
    long sumX = plRow.getLong(0);

    // (3) one heavy field + one light field
    StructType prunedMixed = new StructType()
        .add("heavy", new StructType().add("h1", DataTypes.StringType))
        .add("light", new StructType().add("x", DataTypes.LongType));
    FileSystem.clearStatistics();
    long m0 = bytesRead();
    Row mxRow = spark.read().format("yosegi").schema(prunedMixed).load(path)
        .selectExpr("sum(length(heavy.h1))", "sum(light.x)", "count(1)").first();
    long mixedBytes = bytesRead() - m0;
    long cntMixed = mxRow.getLong(2);

    System.out.println("==== BYTES BENCH n=" + n + " ====");
    System.out.println("file bytes            = " + fileBytes);
    System.out.printf("full read             = %d  (%.1f%% of file)%n", fullBytes, 100.0 * fullBytes / fileBytes);
    System.out.printf("pruned light.{x,y}    = %d  (%.1f%% of file)%n", prunedBytes, 100.0 * prunedBytes / fileBytes);
    System.out.printf("pruned h1 + x         = %d  (%.1f%% of file)%n", mixedBytes, 100.0 * mixedBytes / fileBytes);

    // correctness: sum(x) over 0..n-1
    Assertions.assertEquals((long) n * (n - 1) / 2, sumX);
    Assertions.assertEquals(n, cntFull);
    Assertions.assertEquals(n, cntMixed);
    // the point of the patch: pruned read must be far below full
    Assertions.assertTrue(prunedBytes < fullBytes / 2,
        "pruned read did not shrink: " + prunedBytes + " vs full " + fullBytes);
  }
}
