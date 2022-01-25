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

class SparkConstLongLoaderTest {
  public static <T> boolean isOutOfRange(final T value) {
    if (value instanceof Double) {
      final Double d = (Double) value;
      return d < Long.MIN_VALUE || d > Long.MAX_VALUE;
    } else if (value instanceof Float) {
      final Float f = (Float) value;
      return f < Long.MIN_VALUE || f > Long.MAX_VALUE;
    }
    final Long l = (Long) value;
    return l < Long.MIN_VALUE || l > Long.MAX_VALUE;
  }

  public <T> void assertLong(
      final T expected, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (expected == null) {
        assertTrue(vector.isNullAt(i));
      } else if (expected instanceof Boolean) {
        assertTrue(vector.isNullAt(i));
      } else if (expected instanceof String) {
        try {
          final Long v = Long.valueOf(String.valueOf(expected));
          assertFalse(vector.isNullAt(i));
          assertEquals(v.longValue(), vector.getLong(i));
        } catch (final Exception e) {
          assertTrue(vector.isNullAt(i));
        }
      } else if (expected instanceof Double) {
        final Double v = (Double) expected;
        if (isOutOfRange(v)) {
          assertTrue(vector.isNullAt(i));
        } else {
          assertFalse(vector.isNullAt(i));
          assertEquals(v.longValue(), vector.getLong(i));
        }
      } else if (expected instanceof Float) {
        final Float v = (Float) expected;
        if (isOutOfRange(v)) {
          assertTrue(vector.isNullAt(i));
        } else {
          assertFalse(vector.isNullAt(i));
          assertEquals(v.longValue(), vector.getLong(i));
        }
      } else {
        final Long v = Long.valueOf(String.valueOf(expected));
        if (isOutOfRange(v)) {
          assertTrue(vector.isNullAt(i));
        } else {
          assertFalse(vector.isNullAt(i));
          assertEquals(v.longValue(), vector.getLong(i));
        }
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
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
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
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromByte_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ByteObj value = new ByteObj(Byte.MIN_VALUE);
    // NOTE: expected
    final Byte expected = Byte.MIN_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromByte_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ByteObj value = new ByteObj(Byte.MAX_VALUE);
    // NOTE: expected
    final Byte expected = Byte.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value =
        new BytesObj(String.valueOf(Byte.MIN_VALUE).getBytes(StandardCharsets.UTF_8));
    // NOTE: expected
    final String expected = String.valueOf(Byte.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value =
        new BytesObj(String.valueOf(Double.MAX_VALUE).getBytes(StandardCharsets.UTF_8));
    // NOTE: expected
    final String expected = String.valueOf(Double.MAX_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_3() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value = new BytesObj("abcdefg".getBytes(StandardCharsets.UTF_8));
    // NOTE: expected
    final String expected = "abcdefg";

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromDouble_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final DoubleObj value = new DoubleObj(-1 * Double.MAX_VALUE);
    // NOTE: expected
    final Double expected = (double) -1 * Double.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromDouble_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final DoubleObj value = new DoubleObj(Double.MAX_VALUE);
    // NOTE: expected
    final Double expected = Double.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromDouble_3() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final DoubleObj value = new DoubleObj(Long.MAX_VALUE);
    // NOTE: expected
    final Double expected = (double) Long.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromFloat_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final FloatObj value = new FloatObj(-1 * Float.MIN_VALUE);
    // NOTE: expected
    final Float expected = (float) -1 * Float.MIN_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromFloat_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final FloatObj value = new FloatObj(Float.MAX_VALUE);
    // NOTE: expected
    final Float expected = Float.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromFloat_3() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final FloatObj value = new FloatObj(Long.MAX_VALUE);
    // NOTE: expected
    final Float expected = (float) Long.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromInteger_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final IntegerObj value = new IntegerObj(Integer.MIN_VALUE);
    // NOTE: expected
    final Integer expected = (int) Integer.MIN_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromInteger_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final IntegerObj value = new IntegerObj(Integer.MAX_VALUE);
    // NOTE: expected
    final Integer expected = Integer.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromLong_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final LongObj value = new LongObj(Long.MIN_VALUE);
    // NOTE: expected
    final Long expected = (long) Long.MIN_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromLong_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final LongObj value = new LongObj(Long.MAX_VALUE);
    // NOTE: expected
    final Long expected = Long.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromShort_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ShortObj value = new ShortObj(Short.MIN_VALUE);
    // NOTE: expected
    final Short expected = (short) Short.MIN_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromShort_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ShortObj value = new ShortObj(Short.MAX_VALUE);
    // NOTE: expected
    final Short expected = Short.MAX_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj(String.valueOf(Byte.MIN_VALUE));
    // NOTE: expected
    final String expected = String.valueOf(Byte.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj(String.valueOf(Double.MAX_VALUE));
    // NOTE: expected
    final String expected = String.valueOf(Double.MAX_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_3() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj("abcdefg");
    // NOTE: expected
    final String expected = "abcdefg";

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertLong(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromNull_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstLongLoader(vector, loadSize);
    loader.setConstFromNull();

    // NOTE: assert
    assertLong(null, vector, loadSize);
  }
}
