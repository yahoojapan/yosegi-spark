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
import jp.co.yahoo.yosegi.spark.test.Utils;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkConstDecimalLoaderTest {
  public <T> void assertDecimal(
      final T expected,
      final WritableColumnVector vector,
      final int loadSize,
      final int precision,
      final int scale) {
    for (int i = 0; i < loadSize; i++) {
      if (expected == null) {
        assertTrue(vector.isNullAt(i));
      } else if (expected instanceof Boolean) {
        assertTrue(vector.isNullAt(i));
      } else if (expected instanceof String) {
        try {
          final Decimal v = Utils.valueToDecimal(expected, precision, scale);
          assertFalse(vector.isNullAt(i));
          assertEquals(v, vector.getDecimal(i, precision, scale));
          System.out.println(v);
          System.out.println(vector.getDecimal(i, precision, scale));
        } catch (final Exception e) {
          assertTrue(vector.isNullAt(i));
        }
      } else {
        try {
          final Decimal v = Utils.valueToDecimal(expected, precision, scale);
          assertFalse(vector.isNullAt(i));
          assertEquals(v, vector.getDecimal(i, precision, scale));
          // System.out.println(v);
          // System.out.println(vector.getDecimal(i, precision, scale));
        } catch (final Exception e) {
          final int index = i;
          assertThrows(
              ArithmeticException.class,
              () -> {
                vector.getDecimal(index, precision, scale);
              });
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
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Boolean expected = true;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromByte_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ByteObj value = new ByteObj(Byte.MIN_VALUE);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Byte expected = Byte.MIN_VALUE;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromBytes_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final String _value = String.valueOf(Byte.MIN_VALUE);
    final BytesObj value = new BytesObj(_value.getBytes(StandardCharsets.UTF_8));
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final String expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromDouble_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final Double _value = -12345678901.123456789;
    final DoubleObj value = new DoubleObj(_value);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Double expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromFloat_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final Float _value = (float) -12345.12345;
    final FloatObj value = new FloatObj(_value);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Float expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromInteger_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final Integer _value = Integer.MIN_VALUE;
    final IntegerObj value = new IntegerObj(_value);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Integer expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromLong_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final Long _value = Long.MIN_VALUE;
    final LongObj value = new LongObj(_value);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Long expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromShort_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final Short _value = Short.MIN_VALUE;
    final ShortObj value = new ShortObj(_value);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Short expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }

  @Test
  void T_setConstFromString_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final String _value = String.valueOf(Byte.MIN_VALUE);
    final StringObj value = new StringObj(_value);
    // NOTE: expected
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final String expected = _value;

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(expected, vector, loadSize, precision, scale);
  }
}
