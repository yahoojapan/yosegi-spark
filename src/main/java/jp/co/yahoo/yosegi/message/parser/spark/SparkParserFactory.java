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

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.catalyst.util.ArrayData;

import jp.co.yahoo.yosegi.message.parser.IParser;

public final class SparkParserFactory{

  public static IParser get( final DataType schema , final InternalRow row , final int ordinal ) throws IOException{
    if( schema instanceof ArrayType ){
      ArrayType arrayType = (ArrayType)schema;
      ArrayData arrayData = (ArrayData)( row.getArray( ordinal ) );
      return new SparkArrayParser( arrayType , arrayData );
    }
    else if( schema instanceof StructType ){
      StructType structType = (StructType)schema;
      InternalRow childRow = row.getStruct( ordinal , structType.fields().length );
      return new SparkStructParser( structType , childRow );
    }
    else if( schema instanceof MapType ){
      MapType mapType = (MapType)schema;
      MapData mapData = row.getMap( ordinal );
      return new SparkMapParser( mapType , mapData );
    }
    else{
      return new SparkNullParser();
    }
  }

  public static IParser[] getFromArray( final DataType schema , final ArrayData row ) throws IOException{
    if( row == null ){
      return new IParser[0];
    }
    IParser[] result = new IParser[row.numElements()];
    if( schema instanceof ArrayType ){
      ArrayType arrayType = (ArrayType)schema;
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        ArrayData arrayData = row.getArray( ordinal );
        result[ordinal] = new SparkArrayParser( arrayType , arrayData );
      }
    }
    else if( schema instanceof StructType ){
      StructType structType = (StructType)schema;
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        InternalRow childRow = row.getStruct( ordinal , structType.fields().length );
        result[ordinal] = new SparkStructParser( structType , childRow );
      }
    }
    else if( schema instanceof MapType ){
      MapType mapType = (MapType)schema;
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        MapData mapData = row.getMap( ordinal );
        result[ordinal] = new SparkMapParser( mapType , mapData );
      }
    }
    else{
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        result[ordinal] = new SparkNullParser();
      }
    }
    return result;
  }

  public static boolean isParserSchema( final DataType schema ){
    if( schema instanceof ArrayType ){
      return true;
    }
    else if( schema instanceof StructType ){
      return true;
    }
    else if( schema instanceof MapType ){
      return true;
    }
    else{
      return false;
    }
  }

}
