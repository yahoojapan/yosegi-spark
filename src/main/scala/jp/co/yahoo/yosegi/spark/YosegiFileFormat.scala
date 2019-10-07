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
package jp.co.yahoo.yosegi.spark

import java.net.URI
import java.io.BufferedInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.vectorized.ColumnVector

import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode

import jp.co.yahoo.yosegi.spark.schema.SchemaFactory
import jp.co.yahoo.yosegi.spark.reader.IColumnarBatchReader
import jp.co.yahoo.yosegi.spark.reader.SparkColumnarBatchReader
import jp.co.yahoo.yosegi.spark.reader.SparkArrowColumnarBatchReader
import jp.co.yahoo.yosegi.spark.utils.ProjectionPushdownUtil
import jp.co.yahoo.yosegi.spark.pushdown.FilterConnectorFactory

class YosegiFileFormat extends FileFormat with DataSourceRegister with Serializable{

  override def shortName(): String = "yosegi"

  override def toString: String = "Yosegi"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[YosegiFileFormat]

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  override def inferSchema(
      sparkSession: SparkSession, 
      options: Map[String, String], 
      files: Seq[FileStatus] ): Option[StructType] = {
    val expandOption:Option[String] = options.get( "spread.reader.expand.column" )
    val flattenOption:Option[String] = options.get( "spread.reader.flatten.column" )
    var yosegiConfig = new jp.co.yahoo.yosegi.config.Configuration()
    if( expandOption.nonEmpty ){
      yosegiConfig.set( "spread.reader.expand.column" , expandOption.get )
    }
    if( flattenOption.nonEmpty ){
      yosegiConfig.set( "spread.reader.flatten.column" , flattenOption.get )
    }
    Some( SchemaFactory.create( sparkSession , yosegiConfig , files.toArray ) )
  }

  override def prepareWrite(
        sparkSession: SparkSession, 
        job: Job, 
        options: Map[String, String], 
        dataSchema: StructType ): OutputWriterFactory = {
    // TODO:Set writer option
    new YosegiOutputWriterFactory()
  }

  override def supportBatch( sparkSession: SparkSession, schema: StructType ): Boolean = {
    true
  }

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    Option(
      Seq.fill(requiredSchema.fields.length + partitionSchema.fields.length)( classOf[ColumnVector].getName ) 
    )
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession, 
      dataSchema: StructType, 
      partitionSchema: StructType,
      requiredSchema: StructType, 
      filters: Seq[Filter], 
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val projectionPushdownJson = ProjectionPushdownUtil.createProjectionPushdownJson( requiredSchema )
    val requiredSchemaJson = requiredSchema.json
    val partitionSchemaJson = partitionSchema.json
    val expandOption:Option[String] = options.get( "spread.reader.expand.column" ) 
    val flattenOption:Option[String] = options.get( "spread.reader.flatten.column" )
    val enableArrowReader:Option[String] = options.get( "spark.yosegi.enable.arrow.reader" )
    ( file: PartitionedFile ) => {
      val node = new AndExpressionNode()
      filters.map( FilterConnectorFactory.get( _ ) ).filter( _ != null ).foreach( node.addChildNode( _ ) )
      val readSchema:DataType = DataType.fromJson( requiredSchemaJson )
      val partSchema:DataType = DataType.fromJson( partitionSchemaJson )
      assert(file.partitionValues.numFields == partitionSchema.size )
      val path:Path = new Path( new URI(file.filePath) ) 
      val fs:FileSystem = path.getFileSystem( broadcastedHadoopConf.value.value )
      val yosegiConfig = new jp.co.yahoo.yosegi.config.Configuration()
      if( expandOption.nonEmpty ){
        yosegiConfig.set( "spread.reader.expand.column" , expandOption.get )
      }
      if( flattenOption.nonEmpty ){
        yosegiConfig.set( "spread.reader.flatten.column" , flattenOption.get )
      }
      yosegiConfig.set( "spread.reader.read.column.names" , projectionPushdownJson );

      var reader:IColumnarBatchReader = null
      if( enableArrowReader.nonEmpty && "true".equals( enableArrowReader.get ) 
          || expandOption.nonEmpty
          || flattenOption.nonEmpty ){
        reader = new SparkArrowColumnarBatchReader( partSchema.asInstanceOf[StructType] , file.partitionValues , readSchema.asInstanceOf[StructType] , fs.open( path ) , fs.getFileStatus( path ).getLen() , file.start , file.length , yosegiConfig , node )
      }
      else{
        reader = new SparkColumnarBatchReader( partSchema.asInstanceOf[StructType] , file.partitionValues , readSchema.asInstanceOf[StructType] , fs.open( path ) , fs.getFileStatus( path ).getLen() , file.start , file.length , yosegiConfig , node )
      }
      reader.setLineFilterNode( node )
      val itr = new InternalRowIterator( reader )
      Option( TaskContext.get() ).foreach( _.addTaskCompletionListener( _ => itr.close() ) )

      itr.asInstanceOf[Iterator[InternalRow]]
    }
  }
} 
