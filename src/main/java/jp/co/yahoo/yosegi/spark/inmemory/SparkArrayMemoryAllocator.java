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
package jp.co.yahoo.yosegi.spark.inmemory;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.ColumnVector;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.NullMemoryAllocator;

public class SparkArrayMemoryAllocator implements IMemoryAllocator{

  private final ColumnVector vector;
  private final int vectorSize;

  public SparkArrayMemoryAllocator( final ColumnVector vector , final int vectorSize ){
    this.vector = vector;
    this.vectorSize = vectorSize;
  }

  @Override
  public void setNull( final int index ){
    vector.putNull( index );
  }

  @Override
  public void setValueCount( final int count ) throws IOException{
    for( int i = count ; i < vectorSize ; i++ ){
      setNull( i );
    }
  }

  @Override
  public int getValueCount() throws IOException{
    return vectorSize;
  }

  @Override
  public IMemoryAllocator getArrayChild( final int childLength , final ColumnType type ) throws IOException{
    vector.getChildColumn(0).reserve( childLength );
    return SparkMemoryAllocatorFactory.get( vector.getChildColumn(0) , childLength );
  }

  @Override
  public void setArrayIndex( final int index , final int start , final int length ) throws IOException{
    vector.putArray( index , start , length );
  }

}
