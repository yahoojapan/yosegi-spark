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
package jp.co.yahoo.yosegi.spark

import java.io.Closeable
import java.util.Collections

import jp.co.yahoo.yosegi.spark.reader.IColumnarBatchReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType

class InternalRowIterator( reader:IColumnarBatchReader , returningBatch:Boolean , rowSchema:StructType )
    extends Iterator[Object] with Closeable{

  def this( reader:IColumnarBatchReader ) = this( reader , true , new StructType() )

  private var rows:java.util.Iterator[InternalRow] = Collections.emptyList[InternalRow]().iterator()
  private val projection = if( returningBatch ) None else Some( UnsafeProjection.create( rowSchema ) )

  override def hasNext(): Boolean = {
    if( returningBatch ){
      reader.hasNext()
    } else {
      while( !rows.hasNext() && reader.hasNext() ){
        rows = reader.next().rowIterator()
      }
      rows.hasNext()
    }
  }

  override def next(): Object = {
    if( returningBatch ){
      reader.next()
    } else {
      if( !hasNext() ){
        throw new NoSuchElementException()
      }
      // Both the reader's vectors and UnsafeProjection's result are reused for later rows.
      projection.get( rows.next() ).copy()
    }
  }

  override def close(): Unit = {
    reader.close()
  }

}
