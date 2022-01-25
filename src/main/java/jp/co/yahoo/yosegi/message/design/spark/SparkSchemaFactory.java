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
package jp.co.yahoo.yosegi.message.design.spark;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.sql.types.*;

import jp.co.yahoo.yosegi.message.design.*;

public class SparkSchemaFactory{

  public static DataType getDataType( final IField schema ) throws IOException{
    switch( schema.getFieldType() ){
      case ARRAY:
        IField childSchema = ( (ArrayContainerField)schema ).getField();
        if( childSchema == null ){
          return DataTypes.NullType;
        }
        return DataTypes.createArrayType( getDataType( childSchema ) , true );
      case MAP:
        return DataTypes.createMapType( DataTypes.StringType , getDataType( ( (MapContainerField)schema ).getField() ) , true );
      case STRUCT:
        StructContainerField structFiled = (StructContainerField)schema;
        String[] keys = structFiled.getKeys();
        StructField[] fields = new StructField[keys.length];
        for( int i = 0 ; i < keys.length ; i++ ){
          fields[i] = DataTypes.createStructField( keys[i] , getDataType( structFiled.get( keys[i] ) ) , true );
        }
        return DataTypes.createStructType( fields );
      case BOOLEAN:
        return DataTypes.BooleanType;
      case BYTE:
        return DataTypes.ByteType;
      case BYTES:
        return DataTypes.BinaryType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case FLOAT:
        return DataTypes.FloatType;
      case INTEGER:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case SHORT:
        return DataTypes.ShortType;
      case STRING:
        return DataTypes.StringType;

      default :
        return DataTypes.NullType;
    }
  }

  public static StructType getSparkSchema( final IField schema ) throws IOException{
    if( schema.getFieldType() != FieldType.STRUCT ){
      throw new UnsupportedOperationException( String.format( "Schema root is struct only. Root type is %s." , schema.getFieldType().toString() ) );
    }
    StructContainerField structFiled = (StructContainerField)schema;
    String[] keys = structFiled.getKeys();
    StructField[] fields = new StructField[keys.length];
    for( int i = 0 ; i < keys.length ; i++ ){
      fields[i] = DataTypes.createStructField( keys[i] , getDataType( structFiled.get( keys[i] ) ) , true );
    }
    return DataTypes.createStructType( fields );
  }

  public static IField getGenericSchema( final String name , final DataType schema ) throws IOException{
    if( schema instanceof ArrayType ){
      ArrayType arrayType = (ArrayType)schema;
      IField childSchema = getGenericSchema( name , arrayType.elementType() );
      return new ArrayContainerField( name , childSchema );
    }
    else if( schema instanceof StructType ){
      StructType structType = (StructType)schema;
      StructContainerField result = new StructContainerField( name );
      for( StructField field : structType.fields() ){
        IField childSchema = getGenericSchema( field.name() , field.dataType() );
        result.set( childSchema );
      }
      return result;
    }
    else if( schema instanceof MapType ){
      MapType mapType = (MapType)schema;
      IField childSchema = getGenericSchema( name , mapType.valueType() );
      return new MapContainerField( name , childSchema );
    }
    else if( schema instanceof StringType ){
      return new StringField( name );
    }
    else if( schema instanceof BinaryType ){
      return new BytesField( name );
    }
    else if( schema instanceof BooleanType ){
      return new BooleanField( name );
    }
    else if( schema instanceof ByteType ){
      return new ByteField( name );
    }
    else if( schema instanceof ShortType ){
      return new ShortField( name );
    }
    else if( schema instanceof IntegerType ){
      return new IntegerField( name );
    }
    else if( schema instanceof LongType ){
      return new LongField( name );
    }
    else if( schema instanceof FloatType ){
      return new FloatField( name );
    }
    else if( schema instanceof DoubleType ){
      return new DoubleField( name );
    }
    else{
      return new NullField( name );
    }
  }

}
