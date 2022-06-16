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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.ConstantColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkConstBooleanLoaderTest {
  public void assertBoolean(
      final Boolean expected, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (expected == null) {
        assertTrue(vector.isNullAt(i));
      } else {
        assertFalse(vector.isNullAt(i));
        assertEquals(expected, vector.getBoolean(i));
      }
    }
  }

  @Test
  void T_setConstFromBoolean_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BooleanObj value = new BooleanObj(true);
    // NOTE: expected
    final Boolean expected = true;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBoolean_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BooleanObj value = new BooleanObj(false);
    // NOTE: expected
    final Boolean expected = false;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromByte_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ByteObj value = new ByteObj(Byte.MIN_VALUE);
    // NOTE: expected
    final Boolean expected = null;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value = new BytesObj("true".getBytes(StandardCharsets.UTF_8));
    // NOTE: expected
    final Boolean expected = true;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value = new BytesObj("false".getBytes(StandardCharsets.UTF_8));
    // NOTE: expected
    final Boolean expected = false;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromDouble_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final DoubleObj value = new DoubleObj(Double.MIN_VALUE);
    // NOTE: expected
    final Boolean expected = null;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromFloat_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final FloatObj value = new FloatObj(Float.MIN_VALUE);
    // NOTE: expected
    final Boolean expected = null;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromInteger_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final IntegerObj value = new IntegerObj(Integer.MIN_VALUE);
    // NOTE: expected
    final Boolean expected = null;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromLong_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final LongObj value = new LongObj(Long.MIN_VALUE);
    // NOTE: expected
    final Boolean expected = null;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromShort_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ShortObj value = new ShortObj(Short.MIN_VALUE);
    // NOTE: expected
    final Boolean expected = null;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj("true");
    // NOTE: expected
    final Boolean expected = true;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj("false");
    // NOTE: expected
    final Boolean expected = false;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromNull_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromNull();

    // NOTE: assert
    assertBoolean(null, vector, loadSize);
  }
}
