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

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.Decimal;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

public class SparkDecimalMemoryAllocator implements IMemoryAllocator{

  private final ColumnVector vector;
  private final int vectorSize;

  public SparkDecimalMemoryAllocator( final ColumnVector vector , final int vectorSize ){
    this.vector = vector;
    this.vectorSize = vectorSize;
  }

  @Override
  public void setNull( final int index ){
    vector.putNull( index );
  }

  @Override
  public void setBoolean( final int index , final boolean value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setBoolean()" );
  }

  @Override
  public void setByte( final int index , final byte value ) throws IOException{
    setInteger( index , (int)value );
  }

  @Override
  public void setShort( final int index , final short value ) throws IOException{
    setInteger( index , (int)value );
  }

  @Override
  public void setInteger( final int index , final int value ) throws IOException{
    Decimal sparkDecimal = Decimal.apply( value );
    vector.putDecimal( index , sparkDecimal , sparkDecimal.precision() );
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException{
    Decimal sparkDecimal = Decimal.apply( value );
    vector.putDecimal( index , sparkDecimal , sparkDecimal.precision() );
  }

  @Override
  public void setFloat( final int index , final float value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setFloat()" );
  }

  @Override
  public void setDouble( final int index , final double value ) throws IOException{
    Decimal sparkDecimal = Decimal.apply( value );
    vector.putDecimal( index , sparkDecimal , sparkDecimal.precision() );
  }

  @Override
  public void setBytes( final int index , final byte[] value ) throws IOException{
    setBytes( index , value , 0 , value.length );
  }

  @Override
  public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
    setPrimitiveObject( index , new BytesObj( value , start , length ) );
  }

  @Override
  public void setString( final int index , final String value ) throws IOException{
    Decimal sparkDecimal = Decimal.apply( value );
    vector.putDecimal( index , sparkDecimal , sparkDecimal.precision() );
  }

  @Override
  public void setString( final int index , final char[] value ) throws IOException{
    setString( index , new String( value ) );
  }

  @Override
  public void setString( final int index , final char[] value , final int start , final int length ) throws IOException{
    setString( index , new String( value , start , length ) );
  }

  @Override
  public void setPrimitiveObject( final int index , final PrimitiveObject value ) throws IOException{
    if( value == null ){
      setNull( index );
    }
    else{
      try{
        setString( index , value.getString() );
      }catch( Exception e ){
        setNull( index );
      }
    }
  }

  @Override
  public void setArrayIndex( final int index , final int start , final int length ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setArrayIndex()" );
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
  public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method getChild()" );
  }

}
