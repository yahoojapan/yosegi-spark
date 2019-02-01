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
package jp.co.yahoo.yosegi.message.objects.spark;

import java.io.IOException;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;

import jp.co.yahoo.yosegi.message.objects.*;

public final class SparkRowToPrimitiveObject{

  private SparkRowToPrimitiveObject(){}

  public static PrimitiveObject get( final DataType schema , final InternalRow row , final int ordinal ) throws IOException{
    if( row == null || row.isNullAt( ordinal ) ){
      return NullObj.getInstance();
    }

    if( schema instanceof StringType ){
      return new StringObj( row.getUTF8String( ordinal ).toString() );
    }
    else if( schema instanceof BinaryType ){
      return new BytesObj( row.getBinary( ordinal ) );
    }
    else if( schema instanceof BooleanType ){
      return new BooleanObj( row.getBoolean( ordinal ) );
    }
    else if( schema instanceof ByteType ){
      return new ByteObj( row.getByte( ordinal ) );
    }
    else if( schema instanceof ShortType ){
      return new ShortObj( row.getShort( ordinal ) );
    }
    else if( schema instanceof IntegerType ){
      return new IntegerObj( row.getInt( ordinal ) );
    }
    else if( schema instanceof LongType ){
      return new LongObj( row.getLong( ordinal ) );
    }
    else if( schema instanceof FloatType ){
      return new FloatObj( row.getFloat( ordinal ) );
    }
    else if( schema instanceof DoubleType ){
      return new DoubleObj( row.getDouble( ordinal ) );
    }
    else{
      return NullObj.getInstance();
    }
  }

  public static PrimitiveObject[] getFromArray( final DataType schema , final ArrayData row ) throws IOException{
    if( row == null ){
      return new PrimitiveObject[0];
    }
    PrimitiveObject[] result = new PrimitiveObject[row.numElements()];
    if( schema instanceof StringType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new StringObj( row.getUTF8String( ordinal ).toString() );
      }
    }
    else if( schema instanceof BinaryType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        result[ordinal] = new BytesObj( row.getBinary( ordinal ) ); 
      }
    }
    else if( schema instanceof BooleanType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new BooleanObj( row.getBoolean( ordinal ) );
      }
    }
    else if( schema instanceof ByteType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new ByteObj( row.getByte( ordinal ) );
      }
    }
    else if( schema instanceof ShortType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new ShortObj( row.getShort( ordinal ) );
      }
    }
    else if( schema instanceof IntegerType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new IntegerObj( row.getInt( ordinal ) );
      }
    }
    else if( schema instanceof LongType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new LongObj( row.getLong( ordinal ) );
      }
    }
    else if( schema instanceof FloatType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new FloatObj( row.getFloat( ordinal ) );
      }
    }
    else if( schema instanceof DoubleType ){
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
        if( row.isNullAt( ordinal ) ){
          result[ordinal] = NullObj.getInstance();
          continue;
        }
        result[ordinal] = new DoubleObj( row.getDouble( ordinal ) );
      }
    }
    else{
      for( int ordinal = 0 ; ordinal < result.length ; ordinal++ ){
       result[ordinal] = NullObj.getInstance();
      }
    }
    return result;
  }

}
