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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;

public final class PartitionColumnUtil{

  public static OnHeapColumnVector[] createPartitionColumns( final StructType partitionColumns , final InternalRow partitionValues , final int rowCount ){
    StructField[] fields = partitionColumns.fields();
    OnHeapColumnVector[] vectors = new OnHeapColumnVector[ fields.length ];
    for( int i = 0 ; i < vectors.length ; i++ ){
      vectors[i] = new OnHeapColumnVector( rowCount , fields[i].dataType() );
      ColumnVectorUtils.populate( vectors[i] , partitionValues , i );
    }
    return vectors;
  }

}
