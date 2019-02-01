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
package jp.co.yahoo.yosegi.spark.reader;

import java.io.IOException;
import java.io.InputStream;

import org.apache.arrow.vector.complex.StructVector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.spark.SparkSchemaFactory;

import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.reader.YosegiArrowReader;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import jp.co.yahoo.yosegi.spark.utils.PartitionColumnUtil;

public class SparkArrowColumnarBatchReader implements IColumnarBatchReader{

  private final YosegiArrowReader reader;
  private final StructType partitionSchema;
  private final InternalRow partitionValue;

  public SparkArrowColumnarBatchReader( 
      final StructType partitionSchema , 
      final InternalRow partitionValue , 
      final StructType readSchema , 
      final InputStream in , 
      final long fileLength , 
      final long start , 
      final long length , 
      final Configuration config , 
      final IExpressionNode node ) throws IOException{
    this.partitionSchema = partitionSchema;
    this.partitionValue = partitionValue;
    YosegiReader innnerReader = new YosegiReader();
    innnerReader.setBlockSkipIndex( node );
    innnerReader.setNewStream( in , fileLength , config , start , length );

    StructContainerField schema = (StructContainerField)( SparkSchemaFactory.getGenericSchema( "root" , readSchema ) );
    reader = new YosegiArrowReader( schema , innnerReader , config );
  }

  @Override
  public void setLineFilterNode( final IExpressionNode node ){
    reader.setNode( node );
  }

  @Override
  public boolean hasNext() throws IOException{
    return reader.hasNext();
  }

  @Override
  public ColumnarBatch next() throws IOException{
    StructVector mapVector = (StructVector)( reader.next() );
    ColumnVector[] childColumns = new ColumnVector[mapVector.size()+partitionSchema.length()];
    for( int i = 0; i < mapVector.size(); i++ ){
      childColumns[i] = new ArrowColumnVector( mapVector.getVectorById(i) );
    }
    ColumnVector[] partColumns = PartitionColumnUtil.createPartitionColumns( partitionSchema , partitionValue , mapVector.valueCount );
    for( int i = mapVector.size() ; i < childColumns.length ; i++ ){
      childColumns[i] = partColumns[i - mapVector.size() ];
    }
    ColumnarBatch result = new ColumnarBatch( childColumns );
    result.setNumRows( mapVector.valueCount );
    return result;
  }

  @Override
  public void close() throws IOException{
    reader.close();
  }

}
