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

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.maker.*;
import jp.co.yahoo.yosegi.blockindex.*;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import jp.co.yahoo.yosegi.spark.utils.PartitionColumnUtil;
import jp.co.yahoo.yosegi.spark.inmemory.SparkMemoryAllocatorFactory;

public class SparkColumnarBatchReader implements IColumnarBatchReader{

  private final YosegiReader reader;
  private final StructType schema;
  private final StructType partitionSchema;
  private final InternalRow partitionValue;
  private final IExpressionNode node;
  private final ColumnarBatch result;
  private final WritableColumnVector[] childColumns;
  private final StructField[] fields;
  private final Map<String,Integer> keyIndexMap = new HashMap<String,Integer>();

  private int currentSpreadSize = 0;

  public SparkColumnarBatchReader( 
      final StructType partitionSchema , 
      final InternalRow partitionValue , 
      final StructType schema , 
      final InputStream in , 
      final long fileLength , 
      final long start , 
      final long length , 
      final Configuration config , 
      final IExpressionNode node ) throws IOException{
    this.schema = schema;
    this.partitionSchema = partitionSchema;
    this.partitionValue = partitionValue;
    this.node = node;
    reader = new YosegiReader();
    reader.setBlockSkipIndex( node );
    reader.setNewStream( in , fileLength , config , start , length );
    childColumns = new OnHeapColumnVector[schema.length()+partitionSchema.length()];
    result = new ColumnarBatch( childColumns );
    fields = schema.fields();
    for( int i = 0 ; i < fields.length ; i++ ){
      keyIndexMap.put( fields[i].name() , i );
      childColumns[i] = new OnHeapColumnVector( 0 , fields[i].dataType() );
    }
  }

  @Override
  public void setLineFilterNode( final IExpressionNode node ){
  }

  @Override
  public boolean hasNext() throws IOException{
    return reader.hasNext();
  }

  @Override
  public ColumnarBatch next() throws IOException{
    if( ! hasNext() ){
      result.setNumRows( 0 );
      return result;
    }
    List<ColumnBinary> columnBinaryList = reader.nextRaw();
    if( node != null ){
      BlockIndexNode blockIndexNode = new BlockIndexNode();
      for( ColumnBinary columnBinary : columnBinaryList ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( blockIndexNode , columnBinary , 0 );
      }
      List<Integer> blockIndexList = node.getBlockSpreadIndex( blockIndexNode );
      if( blockIndexList != null && blockIndexList.isEmpty() ){
        result.setNumRows( 0 );
        return result;
      }
    }

    int spreadSize = reader.getCurrentSpreadSize();
    for( int i = 0 ; i < childColumns.length ; i++ ){
      childColumns[i].reset();
      childColumns[i].reserve( spreadSize );
    }

    boolean[] isSet = new boolean[childColumns.length];
    for( ColumnBinary columnBinary : columnBinaryList ){
      if( ! keyIndexMap.containsKey( columnBinary.columnName ) ){
        continue;
      }
      int index =  keyIndexMap.get( columnBinary.columnName ).intValue();
      isSet[index] = true;
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
      IMemoryAllocator childMemoryAllocator = SparkMemoryAllocatorFactory.get( childColumns[index] , spreadSize );
      maker.loadInMemoryStorage( columnBinary , childMemoryAllocator );
    }
    for( int i = 0 ; i < childColumns.length ; i++ ){
      if( ! isSet[i] ) {
        childColumns[i].putNulls( 0 , spreadSize );
      }
    }
    WritableColumnVector[] partColumns = PartitionColumnUtil.createPartitionColumns( partitionSchema , partitionValue , spreadSize );
    for( int i = schema.length() , n = 0 ; i < childColumns.length ; i++,n++ ){
      childColumns[i] = partColumns[n];
    }
    result.setNumRows( spreadSize );
    return result;
  }

  @Override
  public void close() throws IOException{
    reader.close();
    for( int i = 0 ; i < childColumns.length ; i++ ){
      if( childColumns[i] != null ){
       childColumns[i].close();
      }
    }
  }

}
