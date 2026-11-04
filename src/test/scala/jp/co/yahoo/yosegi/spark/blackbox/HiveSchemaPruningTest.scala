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
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.types._
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HiveSchemaPruningTest {
  private var spark: SparkSession = _
  private var root: File = _
  private val format = "jp.co.yahoo.yosegi.spark.YosegiFileFormat"
  private val schema = StructType(Seq(
    StructField("record_id", LongType),
    StructField("req", StructType(Seq(StructField("unused", StringType), StructField("id", LongType)))),
    StructField("usr", StructType(Seq(StructField("gender", StringType), StructField("age", IntegerType)))),
    StructField("arr", ArrayType(StructType(Seq(StructField("x", IntegerType), StructField("y", StringType))))),
    StructField("mapping", MapType(StringType, StructType(Seq(StructField("x", IntegerType), StructField("y", StringType))))),
    StructField("unused", StringType)))

  @BeforeAll def setup(): Unit = {
    root = Files.createTempDirectory("yosegi-sql-test").toFile
    spark = SparkSession.builder().master("local[2]").appName("Yosegi Hive schema pruning test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").toURI.toString)
      .config("spark.hadoop.javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=" + new File(root, "metastore").getPath + ";create=true")
      .config("spark.sql.extensions", classOf[YosegiSparkSessionExtensions].getName)
      .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val rows = Seq(
      Row(1L, Row("a", 10L), Row("f", 20), Seq(Row(1, "a")), Map("k" -> Row(2, "b")), "large"),
      Row(2L, Row("b", 11L), Row("m", 30), Seq.empty[Row], Map.empty[String, Row], "large"),
      Row(3L, null, null, null, null, "large"),
      Row(4L, Row("only-unused", null), Row(null, 40), Seq(Row(null, "c")), Map("k" -> Row(null, "d")), "large"))
    val data = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
    val path = new File(root, "data").toURI.toString
    data.write.format(format).save(path)
    createHiveTable("events", schema.toDDL, path)
    createHiveTable("partitioned", schema.toDDL, new File(root, "partitions").toURI.toString,
      "PARTITIONED BY (day STRING)")
    // Metastore locations and values, rather than directory names, are authoritative.
    spark.sql(s"ALTER TABLE partitioned ADD PARTITION (day='2026-11-01') LOCATION '$path'")
    val other = new File(root, "outside-table-root").toURI.toString
    data.filter("record_id = 2").write.format(format).save(other)
    spark.sql(s"ALTER TABLE partitioned ADD PARTITION (day='2026-11-02') LOCATION '$other'")
  }

  @AfterAll def stop(): Unit = if (spark != null) {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def createHiveTable(name: String, ddl: String, path: String, partition: String = ""): Unit =
    spark.sql(s"""CREATE EXTERNAL TABLE $name ($ddl) $partition
      ROW FORMAT SERDE 'jp.co.yahoo.yosegi.hive.YosegiSerde'
      STORED AS INPUTFORMAT 'jp.co.yahoo.yosegi.hive.io.YosegiHiveLineInputFormat'
      OUTPUTFORMAT 'jp.co.yahoo.yosegi.hive.io.YosegiHiveParserOutputFormat'
      LOCATION '$path'""")

  private def query(sql: String, enabled: Boolean = true): DataFrame = {
    spark.conf.set("spark.sql.yosegi.enabled", enabled.toString)
    spark.sql(sql)
  }
  private def scans(df: DataFrame): Seq[FileSourceScanExec] =
    df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s }
  private def sameAsHive(sql: String): DataFrame = {
    val expected = query(sql, false).collect().toSeq
    val df = query(sql)
    assertEquals(expected, df.collect().toSeq, sql)
    df
  }

  @Test def existingHiveSqlPrunesStructFields(): Unit = {
    val df = sameAsHive("SELECT req.id FROM events ORDER BY record_id")
    assertEquals(Seq(Row(10L), Row(11L), Row(null), Row(null)), df.collect().toSeq)
    assertEquals(1, scans(df).size)
    assertEquals("struct<record_id:bigint,req:struct<id:bigint>>", scans(df).head.requiredSchema.catalogString)
  }

  @Test def filterOnlyFieldsAreRead(): Unit = {
    val df = sameAsHive("SELECT req.id FROM events WHERE usr.age >= 30 ORDER BY record_id")
    val read = scans(df).head.requiredSchema.catalogString
    assertTrue(read.contains("usr:struct<age:int>"), read)
    assertFalse(read.contains("gender"), read)
    assertFalse(read.contains("unused"), read)
  }
  @Test def wholeStructSelectionPreservesAllChildren(): Unit = {
    val df = sameAsHive("SELECT req, req.id AS value FROM events ORDER BY record_id")
    assertTrue(scans(df).head.requiredSchema.catalogString.contains("req:struct<unused:string,id:bigint>"))
  }
  @Test def structNullnessAndNullChildrenMatchHive(): Unit = {
    sameAsHive("SELECT record_id, req.id, req IS NULL AS n FROM events ORDER BY record_id")
    sameAsHive("SELECT record_id, req.id FROM events WHERE req IS NOT NULL ORDER BY record_id")
    sameAsHive("SELECT record_id FROM events WHERE req.id IS NULL ORDER BY record_id")
  }
  @Test def arrayElementFieldsArePruned(): Unit = {
    val df = sameAsHive("SELECT arr[0].x FROM events ORDER BY record_id")
    val read = scans(df).head.requiredSchema.catalogString
    assertTrue(read.contains("array<struct<x:int>>"), read)
  }
  @Test def partitionedArrayProjectionKeepsMetastoreValues(): Unit = {
    val df = sameAsHive("SELECT day, item.x FROM partitioned LATERAL VIEW explode(arr) e AS item " +
      "WHERE day='2026-11-01' ORDER BY day, item.x")
    assertEquals(Seq(Row("2026-11-01", null), Row("2026-11-01", 1)), df.collect().toSeq)
    assertTrue(scans(df).head.requiredSchema.catalogString.contains("array<struct<x:int>>"))
    assertTrue(scans(df).head.partitionFilters.nonEmpty)
  }
  @Test def unsupportedMapValuesKeepHivePath(): Unit = {
    val df = sameAsHive("SELECT mapping['k'].x FROM events ORDER BY record_id")
    assertTrue(scans(df).isEmpty)
  }
  @Test def partitionSerdeOverridesKeepHivePath(): Unit = {
    createHiveTable("partition_options", schema.toDDL, new File(root, "partition-options").toURI.toString,
      "PARTITIONED BY (day STRING)")
    val path = new File(root, "data").toURI.toString
    spark.sql(s"ALTER TABLE partition_options ADD PARTITION (day='one') LOCATION '$path'")
    spark.sql("ALTER TABLE partition_options PARTITION (day='one') SET SERDEPROPERTIES ('custom.option'='true')")
    val df = sameAsHive("SELECT req.id FROM partition_options ORDER BY record_id")
    assertTrue(scans(df).isEmpty)
  }
  @Test def unselectedPartitionOverridesDoNotDisableOptimization(): Unit = {
    createHiveTable("mixed_partition_options", schema.toDDL,
      new File(root, "mixed-partition-options").toURI.toString, "PARTITIONED BY (day STRING)")
    val path = new File(root, "data").toURI.toString
    spark.sql(s"ALTER TABLE mixed_partition_options ADD PARTITION (day='one') LOCATION '$path'")
    spark.sql(s"ALTER TABLE mixed_partition_options ADD PARTITION (day='two') LOCATION '$path'")
    spark.sql("ALTER TABLE mixed_partition_options PARTITION (day='two') SET SERDEPROPERTIES ('custom.option'='true')")
    val df = sameAsHive("SELECT req.id FROM mixed_partition_options WHERE day='one' ORDER BY record_id")
    assertEquals(1, scans(df).size)
  }
  @Test def pruningDisabledDoesNotReadUnsupportedSibling(): Unit = {
    val path = new File(root, "mixed-struct").toURI.toString
    val data = spark.sql("SELECT named_struct('id', record_id, 'mapping', mapping) AS payload FROM events")
    data.write.format(format).save(path)
    createHiveTable("mixed_struct", data.schema.toDDL, path)
    assertEquals(1, scans(sameAsHive("SELECT payload.id FROM mixed_struct")).size)
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "false")
    try {
      val df = sameAsHive("SELECT payload.id FROM mixed_struct")
      assertTrue(scans(df).isEmpty)
    } finally spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
  }
  @Test def datePartitionsKeepTheirValues(): Unit = {
    createHiveTable("date_partitions", schema.toDDL, new File(root, "date-partitions").toURI.toString,
      "PARTITIONED BY (day DATE)")
    val path = new File(root, "data").toURI.toString
    spark.sql(s"ALTER TABLE date_partitions ADD PARTITION (day='2026-11-01') LOCATION '$path'")
    val df = sameAsHive("SELECT day, req.id FROM date_partitions WHERE day=DATE '2026-11-01' ORDER BY record_id")
    assertEquals(1, scans(df).size)
  }
  @Test def caseInsensitiveFieldReferencesAndCteWork(): Unit = {
    sameAsHive("WITH chosen AS (SELECT REQ.ID AS v, RECORD_ID FROM events) SELECT v FROM chosen ORDER BY RECORD_ID")
  }
  @Test def emptyPartitionSelectionReadsNoOtherPartition(): Unit = {
    val df = sameAsHive("SELECT req.id FROM partitioned WHERE day='no-such-partition'")
    assertTrue(df.collect().isEmpty)
    assertTrue(scans(df).forall(_.selectedPartitions.isEmpty))
  }
  @Test def sharedPartitionLocationsRetainBothPartitionValues(): Unit = {
    createHiveTable("shared_locations", schema.toDDL,
      new File(root, "shared-location-root").toURI.toString, "PARTITIONED BY (day STRING)")
    val path = new File(root, "data").toURI.toString
    spark.sql(s"ALTER TABLE shared_locations ADD PARTITION (day='one') LOCATION '$path'")
    spark.sql(s"ALTER TABLE shared_locations ADD PARTITION (day='two') LOCATION '$path'")
    val df = sameAsHive("SELECT day, count(*) FROM shared_locations GROUP BY day ORDER BY day")
    assertEquals(Seq(Row("one", 4L), Row("two", 4L)), df.collect().toSeq)
  }
  @Test def adaptiveExecutionKeepsResults(): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    try sameAsHive("""SELECT a.req.id, b.usr.age FROM events a JOIN events b
      ON a.record_id=b.record_id WHERE b.usr.age > 20 ORDER BY a.record_id""")
    finally spark.conf.set("spark.sql.adaptive.enabled", "false")
  }
  @Test def aliasesAndSelfJoinKeepExpressionBindings(): Unit = {
    val df = sameAsHive("""SELECT a.req.id AS first, b.usr.gender AS second
      FROM events a JOIN events b ON a.record_id = b.record_id ORDER BY a.record_id""")
    assertEquals(2, scans(df).size)
    assertEquals(Seq("first", "second"), df.columns.toSeq)
    assertFalse(scans(df).exists(_.requiredSchema.catalogString.contains("unused")))
  }
  @Test def partitionsUseMetastoreLocationsAndValues(): Unit = {
    val df = sameAsHive("""SELECT req.id, day FROM partitioned
      WHERE day = '2026-11-02' ORDER BY record_id""")
    assertEquals(Seq(Row(11L, "2026-11-02")), df.collect().toSeq)
    assertEquals(1, scans(df).head.selectedPartitions.size)
    sameAsHive("SELECT day, count(*) FROM partitioned GROUP BY day ORDER BY day")
  }
  @Test def countAndEmptyProjectionWork(): Unit = {
    sameAsHive("SELECT count(*) FROM events")
    sameAsHive("SELECT 1 FROM events")
  }
  @Test def disableFlagKeepsHivePath(): Unit = {
    val df = query("SELECT req.id FROM events", false)
    assertTrue(scans(df).isEmpty)
    assertTrue(df.queryExecution.analyzed.exists(_.isInstanceOf[HiveTableRelation]))
  }
  @Test def otherProvidersAreNotConverted(): Unit = {
    spark.range(3).write.mode("overwrite").saveAsTable("parquet_events")
    val df = query("SELECT * FROM parquet_events")
    assertFalse(scans(df).exists(_.relation.fileFormat.getClass.getName == format))
  }
  @Test def customSerdeOptionsKeepHivePath(): Unit = {
    createHiveTable("custom_options", schema.toDDL, new File(root, "data").toURI.toString)
    spark.sql("ALTER TABLE custom_options SET SERDEPROPERTIES ('custom.option'='true')")
    val df = query("SELECT req.id FROM custom_options")
    assertTrue(df.queryExecution.analyzed.exists(_.isInstanceOf[HiveTableRelation]))
    assertTrue(scans(df).isEmpty)
  }
  @Test def hiveFlattenPropertyKeepsHiveSemantics(): Unit = {
    createHiveTable("flattened", "req_id BIGINT", new File(root, "data").toURI.toString)
    spark.sql("""ALTER TABLE flattened SET TBLPROPERTIES
      ('yosegi.flatten'='[{"link_name":"req_id","nodes":["req","id"]}]')""")
    val df = sameAsHive("SELECT req_id FROM flattened ORDER BY req_id NULLS FIRST")
    assertEquals(Seq(Row(null), Row(null), Row(10L), Row(11L)), df.collect().toSeq)
    assertTrue(scans(df).isEmpty)
  }
  @Test def hiveExpandPropertyKeepsHivePath(): Unit = {
    createHiveTable("expanded", schema.toDDL, new File(root, "data").toURI.toString)
    spark.sql("""ALTER TABLE expanded SET TBLPROPERTIES
      ('yosegi.expand'='{"base":{"node":"arr","link_name":"expanded"}}')""")
    val df = query("SELECT req.id FROM expanded")
    assertTrue(scans(df).isEmpty)
  }
  @Test def suffixedHiveFlattenPropertyKeepsHiveSemantics(): Unit = {
    createHiveTable("flattened_suffix", "req_id BIGINT", new File(root, "data").toURI.toString)
    spark.sql("""ALTER TABLE flattened_suffix SET TBLPROPERTIES
      ('yosegi.flatten.1'='[{"link_name":"req_id","nodes":["req","id"]}]')""")
    val df = sameAsHive("SELECT req_id FROM flattened_suffix ORDER BY req_id NULLS FIRST")
    assertEquals(Seq(Row(null), Row(null), Row(10L), Row(11L)), df.collect().toSeq)
    assertTrue(scans(df).isEmpty)
  }
  @Test def globalHiveTransformSettingsKeepHivePath(): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val keys = Seq("yosegi.expand", "yosegi.flatten.1", "spread.reader.expand.column", "spread.reader.flatten.column.1")
    keys.foreach { key =>
      val before = Option(conf.get(key))
      conf.set(key, "[]")
      try assertTrue(scans(query("SELECT req.id FROM events")).isEmpty, key)
      finally before match { case Some(value) => conf.set(key, value); case None => conf.unset(key) }
    }
  }
  @Test def writeTargetRemainsHive(): Unit = {
    // Analyze the command without executing a write.
    spark.conf.set("spark.sql.yosegi.enabled", "true")
    val parser = spark.sessionState.sqlParser
    val plan = spark.sessionState.analyzer.execute(parser.parsePlan("INSERT INTO events SELECT * FROM events"))
    assertTrue(plan.getClass.getName.contains("InsertIntoHiveTable"), plan.treeString)
    val optimized = spark.sessionState.optimizer.execute(plan)
    assertTrue(optimized.exists(_.isInstanceOf[HiveTableRelation]), optimized.treeString)
  }
  @Test def physicallyReadsFewerBytes(): Unit = {
    val random = new java.util.Random(7)
    val wideSchema = StructType(Seq(StructField("req", StructType(Seq(
      StructField("id", LongType), StructField("unused", StringType))))))
    val rows = (0 until 256).map { i =>
      val bytes = new Array[Byte](32768)
      random.nextBytes(bytes)
      Row(Row(i.toLong, java.util.Base64.getEncoder.encodeToString(bytes)))
    }
    val path = new File(root, "wide").toURI.toString
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), wideSchema).write.format(format).save(path)
    createHiveTable("wide_events", wideSchema.toDDL, path)
    def measured(enabled: Boolean): (Seq[Row], Long) = {
      val bytes = new AtomicLong()
      val ended = new CountDownLatch(1)
      val listener = new SparkListener {
        override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
          if (e.taskMetrics != null) bytes.addAndGet(e.taskMetrics.inputMetrics.bytesRead)
        override def onJobEnd(e: SparkListenerJobEnd): Unit = ended.countDown()
      }
      spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", enabled.toString)
      val df = query("SELECT req.id FROM wide_events", true)
      assertEquals(format, scans(df).head.relation.fileFormat.getClass.getName)
      df.queryExecution.executedPlan
      spark.sparkContext.addSparkListener(listener)
      try {
        val result = df.collect().toSeq
        assertTrue(ended.await(30, TimeUnit.SECONDS))
        (result, bytes.get())
      } finally spark.sparkContext.removeSparkListener(listener)
    }
    try {
      val (oldRows, oldBytes) = measured(false)
      val (newRows, newBytes) = measured(true)
      assertEquals(oldRows, newRows)
      assertTrue(oldBytes > 0)
      assertTrue(newBytes < oldBytes / 2, s"nestedOff=$oldBytes, nestedOn=$newBytes")
    } finally {
      spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
    }
  }
}
