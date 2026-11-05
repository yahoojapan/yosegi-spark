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
package org.apache.spark.sql.yosegi

import scala.collection.JavaConverters._

import jp.co.yahoo.yosegi.spark.YosegiFileFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.SchemaPruning.{identifyRootFields, pruneSchema}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.execution.PruneHiveTablePartitions
import org.apache.spark.sql.types._

/** Uses Spark internals deliberately; this module is compiled for the Spark 3.5 line. */
class ConvertYosegiHiveTables( spark:SparkSession ) extends Rule[LogicalPlan]{
  private val serde = "jp.co.yahoo.yosegi.hive.YosegiSerde"
  private val inputFormat = "jp.co.yahoo.yosegi.hive.io.YosegiHiveLineInputFormat"

  override def apply( plan:LogicalPlan ):LogicalPlan = {
    // In particular, never rewrite an INSERT/DDL target into a writable file relation.
    if( !YosegiSQLConf.enabled( spark ) || plan.isInstanceOf[Command] || hasGlobalTransforms ) return plan
    // Pre-CBO runs before Spark's ordinary early scan batch. Invoke its Hive rule
    // here so eligibility checks only fetch metadata for statically selected partitions.
    new PruneHiveTablePartitions( spark ).apply( plan ).transformDown {
      case op @ ScanOperation( projects , stayUp , pushDown , hive:HiveTableRelation )
          if supported( hive ) && requestedTypesSupported(
            hive.tableMeta.dataSchema , projects , stayUp ++ pushDown ) =>
        val table = hive.tableMeta
        val partitions = if( hive.isPartitioned ) hive.prunedPartitions.getOrElse(
          spark.sessionState.catalog.listPartitions( table.identifier ) ) else Seq.empty
        if( !partitions.forall( p => standardStorage( p.storage ) ) ) op
        else {
          val index = if( hive.isPartitioned ) partitionIndex( hive , partitions )
            else new CatalogFileIndex( spark , table , hive.stats.sizeInBytes.min( Long.MaxValue ).toLong )
          val fs = HadoopFsRelation( index , table.partitionSchema , table.dataSchema ,
            None , new YosegiFileFormat , table.storage.properties )( spark )
          val relation = LogicalRelation( fs , hive.output , Some( table ) , false )
          relation.copyTagsFrom( hive )
          // Keep the original projects and filters, including their ordering and aliases.
          op.transformDown { case r:HiveTableRelation if r eq hive => relation }
        }
    }
  }

  private def supported( hive:HiveTableRelation ):Boolean = {
    val table = hive.tableMeta
    table.tableType == CatalogTableType.EXTERNAL &&
      table.provider.exists( _.equalsIgnoreCase( "hive" ) ) &&
      table.storage.serde.contains( serde ) && table.storage.inputFormat.contains( inputFormat ) &&
      table.storage.locationUri.nonEmpty && table.bucketSpec.isEmpty &&
      // Custom SerDe options may change Hive semantics; keep those tables on Hive.
      ordinaryProperties( table.storage ) &&
      !table.properties.keys.exists( key =>
        key.startsWith( "spread." ) || isTransformKey( key ) ||
          key == "storage_handler" ||
          key == "transactional" || key.startsWith( "skip." ) )
  }

  private def isTransformKey( key:String ):Boolean =
    key == "yosegi.expand" || key.startsWith( "yosegi.flatten" ) ||
      key == "spread.reader.expand.column" || key.startsWith( "spread.reader.flatten.column" )

  private def hasGlobalTransforms:Boolean =
    spark.sessionState.newHadoopConf().iterator().asScala.exists( e => isTransformKey( e.getKey ) )

  private def ordinaryProperties( storage:CatalogStorageFormat ):Boolean =
    storage.properties.forall { case ( key , value ) => key == "serialization.format" && value == "1" }

  private def standardStorage( storage:CatalogStorageFormat ):Boolean =
    storage.serde.contains( serde ) && storage.inputFormat.contains( inputFormat ) &&
      ordinaryProperties( storage )

  private def partitionIndex( hive:HiveTableRelation , partitions:Seq[CatalogTablePartition] ):FileIndex = {
    val partitionSchema = hive.tableMeta.partitionSchema
    val hadoopConf = spark.sessionState.newHadoopConf()
    val paths = partitions.map { p =>
      val path = new Path( p.location )
      val fs = path.getFileSystem( hadoopConf )
      PartitionPath( p.toRow( partitionSchema , spark.sessionState.conf.sessionLocalTimeZone ) ,
        path.makeQualified( fs.getUri , fs.getWorkingDirectory ) )
    }
    // Freeze exactly the partition storage entries that were validated. Preserve further
    // static/dynamic pruning using Spark's ordinary InMemoryFileIndex implementation.
    new InMemoryFileIndex( spark , paths.map( _.path ) , Map.empty , Some( partitionSchema ) ,
      fileStatusCache = FileStatusCache.getOrCreate( spark ) ,
      userSpecifiedPartitionSpec = Some( PartitionSpec( partitionSchema , paths ) ) )
  }

  private def requestedTypesSupported(
      schema:StructType ,
      projects:Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression] ,
      filters:Seq[org.apache.spark.sql.catalyst.expressions.Expression] ):Boolean = {
    val roots = identifyRootFields( projects , filters )
    if( roots.isEmpty ) true
    else {
      val nested = if( spark.sessionState.conf.nestedSchemaPruningEnabled ) pruneSchema( schema , roots )
        else schema
      val required = YosegiReadSchema.keepMapsWhole( nested , schema )
        .asInstanceOf[StructType]
      required.fields.filter( f => roots.exists( r => spark.sessionState.conf.resolver( r.field.name , f.name ) ) )
        .forall( f => YosegiReadSchema.supported( f.dataType ) )
    }
  }
}
