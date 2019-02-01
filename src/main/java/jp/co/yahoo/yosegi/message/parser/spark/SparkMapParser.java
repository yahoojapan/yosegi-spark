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
package jp.co.yahoo.yosegi.message.parser.spark;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.spark.SparkRowToPrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

public class SparkMapParser implements IParser {

  private final String[] keyArray;
  private final Map<String,Integer> keyIndex;
  private final PrimitiveObject[] childObjArray;
  private final IParser[] childParserArray;

  public SparkMapParser( final MapType schema , final MapData row ) throws IOException{
    keyIndex = new HashMap<String,Integer>();
    DataType keySchema = schema.keyType();
    if( ! ( keySchema instanceof StringType ) ){
      childObjArray = new PrimitiveObject[0];
      childParserArray = new IParser[0];
      keyArray = new String[0];
      return;
    }
    PrimitiveObject[] keyObj = SparkRowToPrimitiveObject.getFromArray( keySchema , row.keyArray() );
    keyArray = new String[keyObj.length];
    for( int i = 0 ; i < keyObj.length ; i++ ){
      keyIndex.put( keyObj[i].getString() , i );
      keyArray[i] = keyObj[i].getString();
    }

    DataType childSchema = schema.valueType();
    ArrayData childRow = row.valueArray();
    childObjArray = SparkRowToPrimitiveObject.getFromArray( childSchema , childRow );
    childParserArray = SparkParserFactory.getFromArray( childSchema , childRow );
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException{
    if( ! keyIndex.containsKey( key ) ){
      return NullObj.getInstance();
    }
    return childObjArray[ keyIndex.get(key).intValue() ];
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    return NullObj.getInstance();
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    if( ! keyIndex.containsKey( key ) ){
      return new SparkNullParser();
    }
    return childParserArray[ keyIndex.get(key).intValue() ];
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    return new SparkNullParser();
  }

  @Override
  public String[] getAllKey() throws IOException{
    return keyArray;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return keyIndex.containsKey( key );
  }

  @Override
  public int size() throws IOException{
    return keyIndex.size();
  }

  @Override
  public boolean isArray() throws IOException{
    return false;
  }

  @Override
  public boolean isMap() throws IOException{
    return true;
  }

  @Override
  public boolean isStruct() throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    if( ! keyIndex.containsKey( key ) ){
      return false;
    }
    return ! ( childParserArray[ keyIndex.get(key).intValue() ] instanceof SparkNullParser );
  }

  @Override
  public Object toJavaObject() throws IOException{
    throw new UnsupportedOperationException( "Unsupported toJavaObject()" );
  }

}
