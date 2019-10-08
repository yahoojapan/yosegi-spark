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

public class TestSparkShortMemoryAllocator{

  @Test
  public void T_newInstance_1(){
    ColumnVector vector = ColumnVector.allocate( 5 , new ShortType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkShortMemoryAllocator( vector , 5 );
  }

  @Test
  public void T_set_1() throws IOException{
    ColumnVector vector = ColumnVector.allocate( 5 , new ShortType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkShortMemoryAllocator( vector , 5 );

    allocator.setShort( 0 , (short)1 );
    allocator.setShort( 1 , (short)0 );
    allocator.setShort( 2 , (short)1 );
    allocator.setShort( 3 , (short)0 );
    allocator.setShort( 4 , (short)1 );

    allocator.setValueCount( 5 );

    assertEquals( (short)1 , vector.getShort( 0 ) );
    assertEquals( (short)0 , vector.getShort( 1 ) );
    assertEquals( (short)1 , vector.getShort( 2 ) );
    assertEquals( (short)0 , vector.getShort( 3 ) );
    assertEquals( (short)1 , vector.getShort( 4 ) );
  }

  @Test
  public void T_set_2() throws IOException{
    ColumnVector vector = ColumnVector.allocate( 5 , new ShortType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkShortMemoryAllocator( vector , 5 );

    allocator.setNull( 0 );
    allocator.setNull( 1 );
    allocator.setNull( 2 );
    allocator.setNull( 3 );
    allocator.setShort( 4 , (short)1 );

    allocator.setValueCount( 5 );

    assertTrue( vector.isNullAt( 0 ) );
    assertTrue( vector.isNullAt( 1 ) );
    assertTrue( vector.isNullAt( 2 ) );
    assertTrue( vector.isNullAt( 3 ) );
    assertEquals( (short)1 , vector.getShort( 4 ) );
  }

  @Test
  public void T_set_3() throws IOException{
    ColumnVector vector = ColumnVector.allocate( 5 , new ShortType() , MemoryMode.OFF_HEAP );
    IMemoryAllocator allocator = new SparkShortMemoryAllocator( vector , 5 );

    allocator.setValueCount( 0 );

    assertTrue( vector.isNullAt( 0 ) );
    assertTrue( vector.isNullAt( 1 ) );
    assertTrue( vector.isNullAt( 2 ) );
    assertTrue( vector.isNullAt( 3 ) );
    assertTrue( vector.isNullAt( 4 ) );
  }

}
