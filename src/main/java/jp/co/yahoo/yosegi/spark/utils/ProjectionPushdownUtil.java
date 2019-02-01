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
package jp.co.yahoo.yosegi.spark.utils;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

public final class ProjectionPushdownUtil{

  public static String createProjectionPushdownJson( final StructType requiredSchema ){
    StructField[] fields = requiredSchema.fields();
    StringBuffer buffer = new StringBuffer();
    buffer.append( "[" );
    for( int i = 0 ; i < fields.length ; i++ ){
      if( i != 0){
        buffer.append( "," );
      }
      buffer.append( String.format( "[\"%s\"]" , fields[i].name() ) );
    }
    buffer.append( "]" );
    return buffer.toString();
  }

}
