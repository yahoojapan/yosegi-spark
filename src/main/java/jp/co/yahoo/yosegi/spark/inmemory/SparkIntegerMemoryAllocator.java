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

import org.apache.spark.sql.execution.vectorized.Dictionary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.inmemory.IDictionary;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

public class SparkIntegerMemoryAllocator implements IMemoryAllocator{

  private class SparkIntegerDictionary implements Dictionary,IDictionary {

    private final int[] intArray;

    public SparkIntegerDictionary( final int dicSize ) {
      intArray = new int[dicSize];
    }

    @Override
    public byte[] decodeToBinary( final int id ) {
      throw new UnsupportedOperationException( "decodeToBinary is not supported." );
    }

    @Override
    public int decodeToInt( final int id ) {
      return intArray[id];
    }

    @Override
    public long decodeToLong( final int id ) {
      throw new UnsupportedOperationException( "decodeToLong is not supported." );
    }

    @Override
    public float decodeToFloat( final int id ) {
      throw new UnsupportedOperationException( "decodeToFloat is not supported." );
    }

    @Override
    public double decodeToDouble( final int id ) {
      throw new UnsupportedOperationException( "decodeToDouble is not supported." );
    }

    @Override
    public void setByte(
        final int index ,
        final byte value ) throws IOException {
      setInteger( index , (int)value );
    }

    @Override
    public void setShort(
        final int index ,
        final short value ) throws IOException {
      setInteger( index , (int)value );
    }

    @Override
    public void setInteger(
        final int index ,
        final int value ) throws IOException {
      intArray[index]= value;
    }

    @Override
    public void setLong(
        final int index ,
        final long value ) throws IOException {
      setInteger( index , (int)value );
    }

  }

  private final WritableColumnVector vector;
  private final int vectorSize;
  private WritableColumnVector idxVector;

  public SparkIntegerMemoryAllocator( final WritableColumnVector vector , final int vectorSize ){
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
    vector.putInt( index , value );
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException{
    setInteger( index , (int)value );
  }

  @Override
  public void setFloat( final int index , final float value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setFloat()" );
  }

  @Override
  public void setDouble( final int index , final double value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setDouble()" );
  }

  @Override
  public void setBytes( final int index , final byte[] value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setBytes()" );
  }

  @Override
  public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setBytes()" );
  }

  @Override
  public void setString( final int index , final String value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setString( final int index , final char[] value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setString( final int index , final char[] value , final int start , final int length ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setPrimitiveObject( final int index , final PrimitiveObject value ) throws IOException{
    if( value == null ){
      setNull( index );
    }
    else{
      try{
        setInteger( index , value.getInt() );
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

  @Override
  public IDictionary createDictionary( final int size ) throws IOException {
    idxVector = vector.reserveDictionaryIds( vectorSize );
    SparkIntegerDictionary dic = new SparkIntegerDictionary( size );
    vector.setDictionary( dic );
    return dic;
  }

  @Override
  public void setFromDictionary(
      final int index ,
      final int dicIndex ,
      final IDictionary dic ) throws IOException {
    idxVector.putInt( index , dicIndex );
  }

}
