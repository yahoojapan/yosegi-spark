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
package org.apache.spark.sql.yosegi

import org.apache.spark.sql.types._

private[yosegi] object YosegiReadSchema {
  // These are the types accepted by the Spark 3.5 V1 SparkLoaderFactoryUtil.
  def supported( dataType:DataType ):Boolean = dataType match {
    case s:StructType => s.fields.forall( f => supported( f.dataType ) )
    case ArrayType( element , _ ) => supported( element )
    case MapType( StringType , value , _ ) => value match {
      case _:StructType | _:ArrayType | _:MapType => false
      case _ => supported( value )
    }
    case StringType | BinaryType | BooleanType | ByteType | ShortType | IntegerType |
         LongType | FloatType | DoubleType | TimestampType => true
    case _:DecimalType => true
    case _ => false
  }

  def keepMapsWhole( pruned:DataType , original:DataType ):DataType =
    ( pruned , original ) match {
      case ( _ , _:MapType ) => original
      case ( p:ArrayType , o:ArrayType ) =>
        p.copy( elementType = keepMapsWhole( p.elementType , o.elementType ) )
      case ( p:StructType , o:StructType ) =>
        StructType( p.fields.map( f => f.copy( dataType = keepMapsWhole( f.dataType , o( f.name ).dataType ) ) ) )
      case _ => pruned
    }

}
