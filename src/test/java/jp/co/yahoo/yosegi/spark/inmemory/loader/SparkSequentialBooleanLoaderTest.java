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
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkSequentialBooleanLoaderTest {
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

  public static <T> void assertBoolean(
      final Map<Integer, T> values, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (values.containsKey(i)) {
        // NOTE: Boolean: 1 or 0
        if (values.get(i) instanceof Boolean) {
          if (values.get(i) == null) {
            assertTrue(vector.isNullAt(i));
          } else if (values.containsKey(i)) {
            assertFalse(vector.isNullAt(i));
            assertEquals(Boolean.valueOf(String.valueOf(values.get(i))), vector.getBoolean(i));
          } else {
            assertTrue(vector.isNullAt(i));
          }
        } else if (values.get(i) instanceof String) {
          try {
            final Boolean v = Boolean.valueOf(String.valueOf(values.get(i)));
            assertFalse(vector.isNullAt(i));
            assertEquals(v.booleanValue(), vector.getBoolean(i));
          } catch (final Exception e) {
            assertTrue(vector.isNullAt(i));
          }
        } else if (values.get(i) instanceof Double) {
          assertTrue(vector.isNullAt(i));
        } else if (values.get(i) instanceof Float) {
          assertTrue(vector.isNullAt(i));
        } else {
          assertTrue(vector.isNullAt(i));
        }
      } else {
        assertTrue(vector.isNullAt(i));
      }
    }
  }

  public static <T> void assertExpandBoolean(
      final Map<Integer, T> values, final WritableColumnVector vector, final int[] repetitions) {
    int rowId = 0;
    for (int i = 0; i < repetitions.length; i++) {
      for (int j = 0; j < repetitions[i]; j++) {
        if (values.containsKey(i)) {
          // NOTE: Boolean: 1 or 0
          if (values.get(i) instanceof Boolean) {
            if (values.get(i) == null) {
              assertTrue(vector.isNullAt(rowId));
            } else if (values.containsKey(i)) {
              assertFalse(vector.isNullAt(rowId));
              assertEquals(
                  Boolean.valueOf(String.valueOf(values.get(i))), vector.getBoolean(rowId));
            } else {
              assertTrue(vector.isNullAt(rowId));
            }
          } else if (values.get(i) instanceof String) {
            try {
              final Boolean v = Boolean.valueOf(String.valueOf(values.get(i)));
              assertFalse(vector.isNullAt(rowId));
              assertEquals(v.booleanValue(), vector.getBoolean(rowId));
            } catch (final Exception e) {
              assertTrue(vector.isNullAt(rowId));
            }
          } else if (values.get(i) instanceof Double) {
            assertTrue(vector.isNullAt(rowId));
          } else if (values.get(i) instanceof Float) {
            assertTrue(vector.isNullAt(rowId));
          } else {
            assertTrue(vector.isNullAt(rowId));
          }
        } else {
          assertTrue(vector.isNullAt(rowId));
        }
        rowId++;
      }
    }
  }

  /*
  public static <T> void assertBoolean(
      final Map<Integer, T> values, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (values.containsKey(i)) {
        assertFalse(vector.isNullAt(i));
        assertEquals(Boolean.valueOf(String.valueOf(values.get(i))), vector.getBoolean(i));
      } else {
        assertTrue(vector.isNullAt(i));
      }
    }
  }
   */

  /*
  public static <T> void assertExpandBoolean(
      final Map<Integer, T> values, final WritableColumnVector vector, final int[] repetitions) {
    int rowId = 0;
    for (int i = 0; i < repetitions.length; i++) {
      for (int j = 0; j < repetitions[i]; j++) {
        if (values.containsKey(i)) {
          assertFalse(vector.isNullAt(rowId));
          assertEquals(Boolean.valueOf(String.valueOf(values.get(i))), vector.getBoolean(rowId));
        } else {
          assertTrue(vector.isNullAt(rowId));
        }
        rowId++;
      }
    }
  }
   */

  public static <T> void assertNumberBoolean(
      final Map<Integer, T> values, final WritableColumnVector vector, final int loadSize) {
    // NOTE: assert
    for (int i = 0; i < loadSize; i++) {
      if (values.containsKey(i)) {
        assertFalse(vector.isNullAt(i));
        if (Double.parseDouble(String.valueOf(values.get(i))) == 0) {
          assertFalse(vector.getBoolean(i));
        } else {
          assertTrue(vector.getBoolean(i));
        }
      } else {
        assertTrue(vector.isNullAt(i));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_booleanColumnBinaryMaker")
  void T_setBoolean_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: true,false,true,false,true
    final Map<Integer, Boolean> values =
        new HashMap<Integer, Boolean>() {
          {
            put(0, true);
            put(1, false);
            put(2, true);
            put(3, false);
            put(4, true);
          }
        };
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBooleanColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_booleanColumnBinaryMaker")
  void T_setBoolean_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,null,null,false,null,null
    final int loadSize = 8;
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

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_booleanColumnBinaryMaker")
  void T_setBoolean_3(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null*8
    final int loadSize = 8;
    final Map<Integer, Boolean> values = new HashMap<Integer, Boolean>() {};

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBooleanColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_booleanColumnBinaryMaker")
  void T_setBoolean_4(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null*2,v*3,null,null*2,v*2,null,null
    final int[] repetitions = new int[] {1, 2, 3, 1, 2, 2, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertExpandBoolean(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setByte_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,true,null,null,false,null,null
    final int loadSize = 9;
    final Map<Integer, Byte> values =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(3, Byte.MIN_VALUE);
            put(6, (byte) 0);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toByteColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,null,null,false,null,null
    final int loadSize = 8;
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, "true");
            put(5, "false");
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  void T_setDouble_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,true,null,null,false,null,null
    final int loadSize = 9;
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, -1 * Double.MAX_VALUE);
            put(3, Double.MAX_VALUE);
            put(6, 0d);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_floatColumnBinaryMaker")
  void T_setFloat_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,true,null,null,false,null,null
    final int loadSize = 9;
    final Map<Integer, Float> values =
        new HashMap<Integer, Float>() {
          {
            put(2, Float.MAX_VALUE);
            put(3, Float.MIN_VALUE);
            put(6, 0f);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toFloatColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,null,null,false,null,null
    final int loadSize = 8;
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, "true");
            put(5, "false");
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setInteger_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,true,null,null,false,null,null
    final int loadSize = 9;
    final Map<Integer, Integer> values =
        new HashMap<Integer, Integer>() {
          {
            put(2, Integer.MAX_VALUE);
            put(3, Integer.MIN_VALUE);
            put(6, 0);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toIntegerColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setLong_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,true,null,null,false,null,null
    final int loadSize = 9;
    final Map<Integer, Long> values =
        new HashMap<Integer, Long>() {
          {
            put(2, Long.MAX_VALUE);
            put(3, Long.MIN_VALUE);
            put(6, 0L);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toLongColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setShort_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,true,null,null,false,null,null
    final int loadSize = 9;
    final Map<Integer, Short> values =
        new HashMap<Integer, Short>() {
          {
            put(2, Short.MAX_VALUE);
            put(3, Short.MIN_VALUE);
            put(6, (short) 0);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toShortColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(values, vector, loadSize);
  }
}
