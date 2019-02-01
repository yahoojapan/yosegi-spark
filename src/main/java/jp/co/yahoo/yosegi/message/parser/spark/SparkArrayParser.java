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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.catalyst.util.ArrayData;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.spark.SparkRowToPrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

public class SparkArrayParser implements IParser {

  private final PrimitiveObject[] childObjArray;
  private final IParser[] childParserArray;

  public SparkArrayParser( final ArrayType schema , final ArrayData row ) throws IOException{
    DataType childSchema = schema.elementType();
    
    childObjArray = SparkRowToPrimitiveObject.getFromArray( childSchema , row );
    childParserArray = SparkParserFactory.getFromArray( childSchema , row );
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException{
    return NullObj.getInstance();
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    if( childObjArray.length <= index ){
      return NullObj.getInstance();
    }
    return childObjArray[index];
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    return new SparkNullParser();
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    if( childParserArray.length <= index ){
      return new SparkNullParser();
    }
    
    return childParserArray[index];
  }

  @Override
  public String[] getAllKey() throws IOException{
    return new String[0];
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return false;
  }

  @Override
  public int size() throws IOException{
    return childParserArray.length;
  }

  @Override
  public boolean isArray() throws IOException{
    return true;
  }

  @Override
  public boolean isMap() throws IOException{
    return false;
  }

  @Override
  public boolean isStruct() throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    if( childParserArray.length <= index ){
      return false;
    }
    return ! ( childParserArray[index] instanceof SparkNullParser );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    return false;
  }

  @Override
  public Object toJavaObject() throws IOException{
    throw new UnsupportedOperationException( "Unsupported toJavaObject()" );
  }

}
