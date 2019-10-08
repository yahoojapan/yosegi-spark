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

public class SparkMapMemoryAllocator implements IMemoryAllocator{

  private final ColumnVector vector;
  private final int vectorSize;

  private String[] keys;
  private int createCount;

  public SparkMapMemoryAllocator( final ColumnVector vector , final int vectorSize ){
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
  public void setChildCount( final int childSize ) throws IOException{
    vector.getChildColumn( 0 ).reserve( vectorSize * childSize );
    vector.getChildColumn( 1 ).reserve( vectorSize * childSize );
    keys = new String[ childSize ];
    createCount = 0;
  }

  @Override
  public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
    IMemoryAllocator wrapAllocator = new MapValueMemoryAllocator( keys.length , createCount , SparkMemoryAllocatorFactory.get( vector.getChildColumn( 1 ) , vectorSize ) );
    createCount++;
    return wrapAllocator;
  }

  public class MapValueMemoryAllocator implements IMemoryAllocator{

    private final int totalChildCount;
    private final int valueIndex;
    private final IMemoryAllocator original;

    public MapValueMemoryAllocator( final int totalChildCount , final int valueIndex , final IMemoryAllocator original ){
      this.totalChildCount = totalChildCount;
      this.valueIndex = valueIndex;
      this.original = original;
    }

    @Override
    public void setNull( final int index ){
      original.setNull( index * totalChildCount + valueIndex );
    }

    @Override
    public void setBoolean( final int index , final boolean value ) throws IOException{
      original.setBoolean( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setByte( final int index , final byte value ) throws IOException{
      original.setByte( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setShort( final int index , final short value ) throws IOException{
      original.setShort( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setInteger( final int index , final int value ) throws IOException{
      original.setInteger( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setLong( final int index , final long value ) throws IOException{
      original.setLong( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setFloat( final int index , final float value ) throws IOException{
      original.setFloat( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setDouble( final int index , final double value ) throws IOException{
      original.setDouble( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setBytes( final int index , final byte[] value ) throws IOException{
      original.setBytes( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
      original.setBytes( index * totalChildCount + valueIndex , value , start , length );
    }

    @Override
    public void setString( final int index , final String value ) throws IOException{
      original.setString( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setString( final int index , final char[] value ) throws IOException{
      original.setString( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setString( final int index , final char[] value , final int start , final int length ) throws IOException{
      original.setString( index * totalChildCount + valueIndex , value , start , length );
    }

    @Override
    public void setPrimitiveObject( final int index , final PrimitiveObject value ) throws IOException{
      original.setPrimitiveObject( index * totalChildCount + valueIndex , value );
    }

    @Override
    public void setArrayIndex( final int index , final int start , final int length ) throws IOException{
      original.setArrayIndex( index * totalChildCount + valueIndex , start , length );
    }

    @Override
    public void setValueCount( final int index ) throws IOException{
      original.setValueCount( index * totalChildCount + valueIndex );
    }

    @Override
    public int getValueCount() throws IOException{
      return original.getValueCount();
    }

    @Override
    public void setChildCount( final int childSize ) throws IOException{
      original.setChildCount( childSize );
    }

    @Override
    public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
      return original.getChild( columnName , type );
    }

    @Override
    public IMemoryAllocator getArrayChild( final int childLength , final ColumnType type ) throws IOException{
      return original.getArrayChild( childLength , type );
    }

  }

}
