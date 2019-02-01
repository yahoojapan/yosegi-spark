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

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

public class TestSparkBooleanMemoryAllocator{

  @Test
  public void T_newInstance_1(){
    OnHeapColumnVector vector = new OnHeapColumnVector( 5 , new BooleanType() );
    IMemoryAllocator allocator = new SparkBooleanMemoryAllocator( vector , 5 );
  }

  @Test
  public void T_set_1() throws IOException{
    OnHeapColumnVector vector = new OnHeapColumnVector( 5 , new BooleanType() );
    IMemoryAllocator allocator = new SparkBooleanMemoryAllocator( vector , 5 );

    allocator.setBoolean( 0 , true );
    allocator.setBoolean( 1 , false );
    allocator.setBoolean( 2 , true );
    allocator.setBoolean( 3 , false );
    allocator.setBoolean( 4 , true );

    allocator.setValueCount( 5 );

    assertTrue( vector.getBoolean( 0 ) );
    assertFalse( vector.getBoolean( 1 ) );
    assertTrue( vector.getBoolean( 2 ) );
    assertFalse( vector.getBoolean( 3 ) );
    assertTrue( vector.getBoolean( 4 ) );
  }

  @Test
  public void T_set_2() throws IOException{
    OnHeapColumnVector vector = new OnHeapColumnVector( 5 , new BooleanType() );
    IMemoryAllocator allocator = new SparkBooleanMemoryAllocator( vector , 5 );

    allocator.setNull( 0 );
    allocator.setNull( 1 );
    allocator.setNull( 2 );
    allocator.setNull( 3 );
    allocator.setBoolean( 4 , true );

    allocator.setValueCount( 5 );

    assertTrue( vector.isNullAt( 0 ) );
    assertTrue( vector.isNullAt( 1 ) );
    assertTrue( vector.isNullAt( 2 ) );
    assertTrue( vector.isNullAt( 3 ) );
    assertTrue( vector.getBoolean( 4 ) );
  }

  @Test
  public void T_set_3() throws IOException{
    OnHeapColumnVector vector = new OnHeapColumnVector( 5 , new BooleanType() );
    IMemoryAllocator allocator = new SparkBooleanMemoryAllocator( vector , 5 );

    allocator.setValueCount( 0 );

    assertTrue( vector.isNullAt( 0 ) );
    assertTrue( vector.isNullAt( 1 ) );
    assertTrue( vector.isNullAt( 2 ) );
    assertTrue( vector.isNullAt( 3 ) );
    assertTrue( vector.isNullAt( 4 ) );
  }

}
