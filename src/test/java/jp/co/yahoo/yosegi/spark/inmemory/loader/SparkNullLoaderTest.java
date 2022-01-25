/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkNullLoaderTest {
  public void assertNull(final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      assertTrue(vector.isNullAt(i));
    }
  }

  @Test
  void T_load_Boolean_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Byte_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.ByteType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Bytes_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Double_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.DoubleType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Float_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Integer_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.IntegerType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Long_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_Short_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.ShortType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }

  @Test
  void T_load_String_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader = new SparkNullLoader(vector, loadSize);
    loader.finish();

    // NOTE: assert
    assertNull(vector, loadSize);
  }
}
