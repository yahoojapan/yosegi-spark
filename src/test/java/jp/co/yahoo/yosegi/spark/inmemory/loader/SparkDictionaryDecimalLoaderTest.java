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
import jp.co.yahoo.yosegi.binary.maker.FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDoubleColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBooleanColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpDoubleColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpFloatColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayFloatColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkDictionaryDecimalLoaderTest {
  public static Stream<Arguments> D_booleanColumnBinaryMaker() {
    return Stream.of(
        arguments(FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker.class.getName()),
        arguments(OptimizedNullArrayDumpBooleanColumnBinaryMaker.class.getName()));
  }

  public static Stream<Arguments> D_longColumnBinaryMaker() {
    return Stream.of(
        arguments(RleLongColumnBinaryMaker.class.getName()),
        arguments(OptimizedNullArrayLongColumnBinaryMaker.class.getName()),
        arguments(OptimizedNullArrayDumpLongColumnBinaryMaker.class.getName()));
  }

  public static Stream<Arguments> D_binaryColumnBinaryMaker() {
    return Stream.of(arguments(OptimizedNullArrayDumpBytesColumnBinaryMaker.class.getName()));
  }

  public static Stream<Arguments> D_doubleColumnBinaryMaker() {
    return Stream.of(
        arguments(OptimizedNullArrayDoubleColumnBinaryMaker.class.getName()),
        arguments(OptimizedNullArrayDumpDoubleColumnBinaryMaker.class.getName()));
  }

  public static Stream<Arguments> D_floatColumnBinaryMaker() {
    return Stream.of(
        arguments(OptimizedNullArrayFloatColumnBinaryMaker.class.getName()),
        arguments(OptimizedNullArrayDumpFloatColumnBinaryMaker.class.getName()));
  }

  public <T> void assertDecimal(
      final Map<Integer, T> values,
      final WritableColumnVector vector,
      final int[] repetitions,
      final int precision,
      final int scale) {
    int rowId = 0;
    for (int i = 0; i < repetitions.length; i++) {
      for (int j = 0; j < repetitions[i]; j++) {
        if (values.containsKey(i)) {
          if (values.get(i) instanceof Boolean) {
            assertTrue(vector.isNullAt(rowId));
          } else if (values.get(i) instanceof String) {
            try {
              final Decimal v = Utils.valueToDecimal(values.get(i), precision, scale);
              assertFalse(vector.isNullAt(rowId));
              assertEquals(v, vector.getDecimal(rowId, precision, scale));
              // System.out.println(v);
              // System.out.println(vector.getDecimal(rowId, precision, scale));
            } catch (final Exception e) {
              assertTrue(vector.isNullAt(rowId));
            }
          } else {
            try {
              final Decimal v = Utils.valueToDecimal(values.get(i), precision, scale);
              assertFalse(vector.isNullAt(rowId));
              assertEquals(v, vector.getDecimal(rowId, precision, scale));
              // System.out.println(v);
              // System.out.println(vector.getDecimal(rowId, precision, scale));
            } catch (final Exception e) {
              final int index = rowId;
              assertThrows(
                  ArithmeticException.class,
                  () -> {
                    vector.getDecimal(index, precision, scale);
                  });
            }
          }
        } else {
          assertTrue(vector.isNullAt(rowId));
        }
        rowId++;
      }
    }
  }

  // FIXME: boolean does not have a dictionary load type.
  /*
  @ParameterizedTest
  @MethodSource("D_booleanColumnBinaryMaker")
  void T_setBooleanToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,null,null,false,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MAX_PRECISION();
    final Map<Integer, Boolean> values =
        new HashMap<Integer, Boolean>() {
          {
            put(2, true);
            put(5, false);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBooleanColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }
   */

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setByteToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Byte.MAX_VALUE,null,null,Byte.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, Byte> values =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MIN_VALUE);
            put(5, Byte.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toByteColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytesToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,v,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, String.valueOf(Byte.MIN_VALUE));
            put(5, String.valueOf(Byte.MAX_VALUE));
            put(6, String.valueOf(Long.MIN_VALUE));
            put(7, String.valueOf(Long.MAX_VALUE));
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  void T_setDoubleToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,v,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, (double) Byte.MIN_VALUE);
            put(5, (double) Byte.MAX_VALUE);
            put(6, -1 * Double.MAX_VALUE);
            put(7, Double.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  void T_setDoubleToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null*2,v*3,null,null*2,v*2,v*3,v*4,null,null
    final int[] repetitions = new int[] {1, 2, 3, 1, 2, 2, 3, 4, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, (double) Byte.MIN_VALUE);
            put(5, (double) Byte.MAX_VALUE);
            put(6, -1 * Double.MAX_VALUE);
            put(7, Double.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  // FIXME: float does not have a dictionary load type.
  /*
  @ParameterizedTest
  @MethodSource("D_floatColumnBinaryMaker")
  void T_setFloatToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,v,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MAX_PRECISION();
    final Map<Integer, Float> values =
        new HashMap<Integer, Float>() {
          {
            put(2, (float) Byte.MIN_VALUE);
            put(5, (float) Byte.MAX_VALUE);
            put(6, -1 * Float.MAX_VALUE);
            put(7, Float.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toFloatColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }
   */

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setIntegerToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, Integer> values =
        new HashMap<Integer, Integer>() {
          {
            put(2, (int) Byte.MIN_VALUE);
            put(5, (int) Byte.MAX_VALUE);
            put(6, Integer.MIN_VALUE);
            put(7, Integer.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toIntegerColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setLongToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,v,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, Long> values =
        new HashMap<Integer, Long>() {
          {
            put(2, (long) Byte.MIN_VALUE);
            put(5, (long) Byte.MAX_VALUE);
            put(6, Long.MIN_VALUE);
            put(7, Long.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toLongColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setShortToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,v,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, Short> values =
        new HashMap<Integer, Short>() {
          {
            put(2, (short) Byte.MIN_VALUE);
            put(5, (short) Byte.MAX_VALUE);
            put(6, Short.MIN_VALUE);
            put(7, Short.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toShortColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setStringToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,v,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, String.valueOf(Byte.MIN_VALUE));
            put(5, String.valueOf(Byte.MAX_VALUE));
            put(6, String.valueOf(Long.MIN_VALUE));
            put(7, String.valueOf(Long.MAX_VALUE));
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(values, vector, repetitions, precision, scale);
  }
}
