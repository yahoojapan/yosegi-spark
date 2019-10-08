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
package jp.co.yahoo.yosegi.spark.inmemory;

import java.io.IOException;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.ColumnVector;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

public class TestSparkLongMemoryAllocator{

  @Test
  public void T_newInstance_1(){
    ColumnVector vector = ColumnVector.allocate( 5 , new LongType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkLongMemoryAllocator( vector , 5 );
  }

  @Test
  public void T_set_1() throws IOException{
    ColumnVector vector = ColumnVector.allocate( 5 , new LongType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkLongMemoryAllocator( vector , 5 );

    allocator.setLong( 0 , (long)1 );
    allocator.setLong( 1 , (long)0 );
    allocator.setLong( 2 , (long)1 );
    allocator.setLong( 3 , (long)0 );
    allocator.setLong( 4 , (long)1 );

    allocator.setValueCount( 5 );

    assertEquals( (long)1 , vector.getLong( 0 ) );
    assertEquals( (long)0 , vector.getLong( 1 ) );
    assertEquals( (long)1 , vector.getLong( 2 ) );
    assertEquals( (long)0 , vector.getLong( 3 ) );
    assertEquals( (long)1 , vector.getLong( 4 ) );
  }

  @Test
  public void T_set_2() throws IOException{
    ColumnVector vector = ColumnVector.allocate( 5 , new LongType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkLongMemoryAllocator( vector , 5 );

    allocator.setNull( 0 );
    allocator.setNull( 1 );
    allocator.setNull( 2 );
    allocator.setNull( 3 );
    allocator.setLong( 4 , (long)1 );

    allocator.setValueCount( 5 );

    assertTrue( vector.isNullAt( 0 ) );
    assertTrue( vector.isNullAt( 1 ) );
    assertTrue( vector.isNullAt( 2 ) );
    assertTrue( vector.isNullAt( 3 ) );
    assertEquals( (long)1 , vector.getLong( 4 ) );
  }

  @Test
  public void T_set_3() throws IOException{
    ColumnVector vector = ColumnVector.allocate( 5 , new LongType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkLongMemoryAllocator( vector , 5 );

    allocator.setValueCount( 0 );

    assertTrue( vector.isNullAt( 0 ) );
    assertTrue( vector.isNullAt( 1 ) );
    assertTrue( vector.isNullAt( 2 ) );
    assertTrue( vector.isNullAt( 3 ) );
    assertTrue( vector.isNullAt( 4 ) );
  }

}
