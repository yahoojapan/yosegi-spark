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

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.catalyst.InternalRow;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.spark.SparkRowToPrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

public class SparkStructParser implements IParser {

  private final InternalRow row;
  private final StructType schema;
  private final StructField[] childSchemas;

  public SparkStructParser( final StructType schema , final InternalRow row ){
    this.row = row;
    this.schema = schema;
    childSchemas = schema.fields();
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException{
    try{
      return get( schema.fieldIndex( key ) );
    }catch( IllegalArgumentException e ){
      return NullObj.getInstance();
    }
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    if( childSchemas.length <= index ){
      return NullObj.getInstance();
    }
    return SparkRowToPrimitiveObject.get( childSchemas[index].dataType() , row , index );
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    try{
      return getParser( schema.fieldIndex( key ) );
    }catch( IllegalArgumentException e ){
      return new SparkNullParser();
    }
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    if( childSchemas.length <= index ){
      return new SparkNullParser();
    }
    
    return SparkParserFactory.get( childSchemas[index].dataType() , row , index );
  }

  @Override
  public String[] getAllKey() throws IOException{
    return schema.fieldNames();
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    try{
      return 0 <= schema.fieldIndex( key );
    }catch( IllegalArgumentException e ){
      return false;
    }
  }

  @Override
  public int size() throws IOException{
    return childSchemas.length;
  }

  @Override
  public boolean isArray() throws IOException{
    return false;
  }

  @Override
  public boolean isMap() throws IOException{
    return false;
  }

  @Override
  public boolean isStruct() throws IOException{
    return true;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    if( childSchemas.length <= index ){
      return false;
    }
    return SparkParserFactory.isParserSchema( childSchemas[index].dataType() );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    try{
      return hasParser( schema.fieldIndex( key ) );
    }catch( IllegalArgumentException e ){
      return false;
    }
  }

  @Override
  public Object toJavaObject() throws IOException{
    throw new UnsupportedOperationException( "Unsupported toJavaObject()" );
  }

}
