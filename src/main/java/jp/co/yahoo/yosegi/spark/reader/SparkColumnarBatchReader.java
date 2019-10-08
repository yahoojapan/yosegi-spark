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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.maker.*;
import jp.co.yahoo.yosegi.blockindex.*;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import jp.co.yahoo.yosegi.spark.inmemory.SparkMemoryAllocatorFactory;

public class SparkColumnarBatchReader implements IColumnarBatchReader{

  private final YosegiReader reader;
  private final StructType schema;
  private final StructType partitionSchema;
  private final InternalRow partitionValue;
  private final IExpressionNode node;
  private final StructField[] fields;
  private final Map<String,Integer> keyIndexMap = new HashMap<String,Integer>();

  private StructType batchSchema;
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
    fields = schema.fields();
    for( int i = 0 ; i < fields.length ; i++ ){
      keyIndexMap.put( fields[i].name() , i );
    }

    batchSchema = new StructType();
    for( StructField f: schema.fields() ){
      batchSchema = batchSchema.add(f);
    }
    if( partitionSchema != null ){
      for( StructField f : partitionSchema.fields() ){
        batchSchema = batchSchema.add(f);
      }
    }

    reader.setNewStream( in , fileLength , config , start , length );
  }

  public ColumnarBatch createColumnarBatch( final StructType batchSchema , final StructType schema , final StructType partitionSchema , final InternalRow partitionValue , final int rows ){
    ColumnarBatch result = ColumnarBatch.allocate( batchSchema , MemoryMode.ON_HEAP , rows );
    if( partitionSchema != null ){
      int partitionIdx = schema.fields().length;
      for( int i = 0; i < partitionSchema.fields().length; i++ ){
        ColumnVectorUtils.populate( result.column( i + partitionIdx ) , partitionValue, i );
        result.column( i + partitionIdx ).setIsConstant();
      }
    }
    return result;
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
      ColumnarBatch result = createColumnarBatch( batchSchema , schema , partitionSchema , partitionValue , 0 );
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
        ColumnarBatch result = createColumnarBatch( batchSchema , schema , partitionSchema , partitionValue , 0 );
        return result;
      }
    }

    int spreadSize = reader.getCurrentSpreadSize();
    ColumnarBatch result = createColumnarBatch( batchSchema , schema , partitionSchema , partitionValue , spreadSize );

    for( ColumnBinary columnBinary : columnBinaryList ){
      if( ! keyIndexMap.containsKey( columnBinary.columnName ) ){
        continue;
      }
      int index =  keyIndexMap.get( columnBinary.columnName ).intValue();
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
      ColumnVector childColumn = result.column( index );
      childColumn.reserve( spreadSize );
      IMemoryAllocator childMemoryAllocator = SparkMemoryAllocatorFactory.get( childColumn , spreadSize );
      maker.loadInMemoryStorage( columnBinary , childMemoryAllocator );
    }
    result.setNumRows( spreadSize );
    return result;
  }

  @Override
  public void close() throws IOException{
    reader.close();
  }

}
