/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.blackbox

import jp.co.yahoo.yosegi.spark.YosegiSparkSessionExtensions

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types._
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArraySchemaPruningTest {
  private var spark: SparkSession = _
  private var root: File = _
  private val format = "jp.co.yahoo.yosegi.spark.YosegiFileFormat"
  private val element = StructType(Seq(StructField("unused", StringType),
    StructField("value", LongType), StructField("nested", StructType(Seq(
      StructField("unused", StringType), StructField("value", LongType))))))
  private val schema = StructType(Seq(StructField("id", LongType),
    StructField("items", ArrayType(element))))

  @BeforeAll def setup(): Unit = {
    root = Files.createTempDirectory("yosegi-array-pruning-").toFile
    spark = SparkSession.builder().master("local[2]").appName("Yosegi array pruning test")
      .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2").config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").toURI.toString)
      .config("spark.hadoop.javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=" + new File(root, "metastore").getPath + ";create=true")
      .config("spark.sql.extensions", classOf[YosegiSparkSessionExtensions].getName)
      .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val rows = Seq(Row(1L, null), Row(2L, Seq.empty[Row]), Row(3L, Seq(null)),
      Row(4L, Seq(Row("a", 40L, Row("n", 400L)), Row("b", null, null))),
      Row(5L, Seq(Row("only-unused", null, Row("only-unused", null)))))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
    val path = new File(root, "data").toURI.toString
    df.write.format(format).save(path)
    createHive("array_events", schema, path)
    // Yosegi's writer represents null arrays/elements differently from the original
    // Spark rows. Compare readers over the same stored values, not two encodings.
    spark.conf.set("spark.sql.yosegi.enabled", "false")
    spark.table("array_events").write.parquet(new File(root, "parquet").toURI.toString)
    spark.read.schema(schema).format(format).load(path).createOrReplaceTempView("array_direct")
    spark.read.parquet(new File(root, "parquet").toURI.toString).createOrReplaceTempView("array_parquet")
  }
  private def createHive(name: String, s: StructType, path: String): Unit =
    spark.sql(s"""CREATE EXTERNAL TABLE $name (${s.toDDL})
      ROW FORMAT SERDE 'jp.co.yahoo.yosegi.hive.YosegiSerde'
      STORED AS INPUTFORMAT 'jp.co.yahoo.yosegi.hive.io.YosegiHiveLineInputFormat'
      OUTPUTFORMAT 'jp.co.yahoo.yosegi.hive.io.YosegiHiveParserOutputFormat' LOCATION '$path'""")
  @AfterAll def stop(): Unit = if (spark != null) {
    spark.stop(); SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
  }
  private def query(sql: String, enabled: Boolean): DataFrame = {
    spark.conf.set("spark.sql.yosegi.enabled", enabled.toString); spark.sql(sql)
  }
  private def readSchema(df: DataFrame): String = df.queryExecution.executedPlan.collect {
    case s: FileSourceScanExec => s.requiredSchema.catalogString
  }.head
  private def check(sql: String): DataFrame = {
    val hive = query(sql.replace("SOURCE", "array_events"), false).collect().toSeq
    val parquet = query(sql.replace("SOURCE", "array_parquet"), true).collect().toSeq
    assertEquals(parquet, hive, "Parquet fixture contains the same stored Hive values")
    val df = query(sql.replace("SOURCE", "array_events"), true)
    assertEquals(hive, df.collect().toSeq)
    val direct = query(sql.replace("SOURCE", "array_direct"), true)
    assertEquals(hive, direct.collect().toSeq)
    df
  }
  @Test def explodePrunesNonfirstElementField(): Unit = {
    val df = check("SELECT id, item.value FROM SOURCE LATERAL VIEW explode(items) e AS item ORDER BY id, item.value")
    assertEquals("struct<id:bigint,items:array<struct<value:bigint>>>", readSchema(df))
    assertEquals(Seq(Row(3L, null), Row(4L, null), Row(4L, 40L), Row(5L, null)), df.collect().toSeq)
  }
  @Test def nestedElementStructIsPruned(): Unit = {
    val df = check("SELECT id, item.nested.value FROM SOURCE LATERAL VIEW explode(items) e AS item ORDER BY id, item.nested.value")
    assertFalse(readSchema(df).contains("unused"), readSchema(df))
    val multiple = check("SELECT id, items.value, items.nested.value FROM SOURCE ORDER BY id")
    assertFalse(readSchema(multiple).contains("unused"), readSchema(multiple))
  }
  @Test def arrayIndexAndOuterExplodePreserveShape(): Unit = {
    check("SELECT id, items[0].value, size(items), items IS NULL FROM SOURCE ORDER BY id")
    check("SELECT id, pos, item.value FROM SOURCE LATERAL VIEW posexplode_outer(items) e AS pos, item ORDER BY id, pos")
  }
  @Test def wholeElementAndWholeArrayReferencesStayWhole(): Unit = {
    val whole = check("SELECT id, item, item.value FROM SOURCE LATERAL VIEW explode(items) e AS item ORDER BY id, item.value")
    assertTrue(readSchema(whole).contains("unused"))
    check("SELECT id, items, items[0].value FROM SOURCE ORDER BY id")
    check("SELECT id, item IS NULL, item.nested IS NULL, item.value FROM SOURCE LATERAL VIEW explode(items) e AS item ORDER BY id, item.value")
  }
  @Test def missingElementFieldPreservesArrayCardinality(): Unit = {
    val evolved = StructType(schema.fields.map { f =>
      if (f.name == "items") f.copy(dataType = ArrayType(element.add("missing", LongType))) else f
    })
    val path = new File(root, "data").toURI.toString
    createHive("evolved_array_events", evolved, path)
    val sql = "SELECT id, item.missing FROM evolved_array_events LATERAL VIEW explode(items) e AS item ORDER BY id"
    val baseline = query(sql, false).collect().toSeq
    val df = query(sql, true)
    assertEquals(baseline, df.collect().toSeq)
    assertEquals(4, baseline.size)
    assertTrue(readSchema(df).contains("array<struct<missing:bigint>>"))
  }
  @Test def nestedArrayContainersPreserveShape(): Unit = {
    val nestedSchema = StructType(Seq(StructField("id", LongType),
      StructField("matrix", ArrayType(ArrayType(element)))))
    val rows = Seq(Row(1L, Seq(Seq(Row("a", 17L, null)))),
      Row(2L, Seq(Seq.empty[Row])), Row(3L, Seq(null)), Row(4L, null))
    val path = new File(root, "nested-arrays").toURI.toString
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), nestedSchema).write.format(format).save(path)
    createHive("nested_array_events", nestedSchema, path)
    val sql = "SELECT id, matrix[0][0].value FROM nested_array_events ORDER BY id"
    val baseline = query(sql, false).collect().toSeq
    val df = query(sql, true)
    assertEquals(baseline, df.collect().toSeq)
    assertFalse(readSchema(df).contains("unused"), readSchema(df))
  }
  @Test def unvalidatedReaderOptionsKeepArraysWhole(): Unit = {
    val path = new File(root, "data").toURI.toString
    Seq("spark.yosegi.enable.arrow.reader" -> "true", "spread.reader.expand.column" -> "[]",
      "spread.reader.flatten.column" -> "[]").foreach { case (key, value) =>
      spark.read.schema(schema).format(format).option(key, value).load(path)
        .createOrReplaceTempView("array_reader_option")
      val df = query("SELECT item.value FROM array_reader_option LATERAL VIEW explode(items) e AS item", true)
      val scan = df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s }.head
      assertEquals(format, scan.relation.fileFormat.getClass.getName, key)
      assertTrue(readSchema(df).contains("unused"), key)
    }
  }
  @Test def disablingNestedPruningKeepsArraysWhole(): Unit = {
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "false")
    try {
      val df = check("SELECT id, item.value FROM SOURCE LATERAL VIEW explode(items) e AS item ORDER BY id, item.value")
      assertTrue(readSchema(df).contains("unused"))
    } finally spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
  }
  @Test def physicallyReadsFewerArrayBytes(): Unit = {
    val random = new java.util.Random(73)
    val wideSchema = StructType(Seq(StructField("items", ArrayType(StructType(Seq(
      StructField("unused", StringType), StructField("value", LongType)))))))
    val rows = (0 until 256).map { i =>
      val bytes = new Array[Byte](32768); random.nextBytes(bytes)
      Row(Seq(Row(java.util.Base64.getEncoder.encodeToString(bytes), i.toLong)))
    }
    val path = new File(root, "wide").toURI.toString
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), wideSchema).write.format(format).save(path)
    createHive("wide_array_events", wideSchema, path)
    def measured(enabled: Boolean): (Seq[Row], Long) = {
      val bytes = new AtomicLong(); val ended = new CountDownLatch(1)
      val listener = new SparkListener {
        override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
          if (e.taskMetrics != null && e.taskInfo.successful) bytes.addAndGet(e.taskMetrics.inputMetrics.bytesRead)
        override def onJobEnd(e: SparkListenerJobEnd): Unit = ended.countDown()
      }
      spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", enabled.toString)
      val df = query("SELECT item.value FROM wide_array_events LATERAL VIEW explode(items) e AS item", true)
      val scan = df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s }.head
      assertEquals(format, scan.relation.fileFormat.getClass.getName)
      df.queryExecution.executedPlan
      spark.sparkContext.addSparkListener(listener)
      try {
        val result = df.collect().toSeq
        assertTrue(ended.await(30, TimeUnit.SECONDS)); (result, bytes.get())
      } finally spark.sparkContext.removeSparkListener(listener)
    }
    try {
      val (before, oldBytes) = measured(false)
      val (after, newBytes) = measured(true)
      assertEquals(before, after)
      assertTrue(oldBytes > 0 && newBytes < oldBytes / 2,
        s"nestedOff=$oldBytes, nestedOn=$newBytes")
    } finally {
      spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
    }
  }
}
