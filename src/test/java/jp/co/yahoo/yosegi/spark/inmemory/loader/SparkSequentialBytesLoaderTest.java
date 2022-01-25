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
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkSequentialBytesLoaderTest {
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

  public static <T> void assertBytes(
      final Map<Integer, T> values, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (values.containsKey(i)) {
        assertFalse(vector.isNullAt(i));
        assertArrayEquals(
            String.valueOf(values.get(i)).getBytes(StandardCharsets.UTF_8), vector.getBinary(i));
      } else {
        assertTrue(vector.isNullAt(i));
      }
    }
  }

  public static <T> void assertString(
      final Map<Integer, T> values, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (values.containsKey(i)) {
        assertFalse(vector.isNullAt(i));
        assertEquals(UTF8String.fromString(String.valueOf(values.get(i))), vector.getUTF8String(i));
      } else {
        assertTrue(vector.isNullAt(i));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_booleanColumnBinaryMaker")
  void T_setBoolean_1(final String binaryMakerClassName) throws IOException {
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
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
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
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setByte_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Byte.MAX_VALUE,null,null,Byte.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Byte> values =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(5, Byte.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toByteColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setByte_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Byte.MAX_VALUE,null,null,Byte.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Byte> values =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(5, Byte.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toByteColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(0, "ABCDEFGHIJ");
            put(1, "abcdefghij");
            put(2, "0123456789");
            put(3, "!\"#$%&'()*");
            put(4, "ｦｧｨｩｪｫｬｭｮｯ");
            put(5, "ＡＢＣＤＥＦＧＨＩＪ");
            put(6, "ａｂｃｄｅｆｇｈｉｊ");
            put(7, "０１２３４５６７８９");
            put(8, "！”＃＄％＆’（）＊");
            put(9, "ぁあぃいぅうぇえぉお");
            put(10, "ァアィイゥウェエォオ");
            put(11, "亜唖娃阿哀愛挨姶逢葵");
            put(12, "弌丐丕个丱丶丼丿乂乖");
            put(13, "―ソ能表ЫⅨ噂浬欺圭");
            put(14, "①②③④⑤⑥⑦⑧⑨⑩");
            put(
                15,
                "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93");
            put(
                16,
                "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09");
          }
        };
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int loadSize = 8;
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, "abcdefghij");
            put(5, "!\"#$%&'()*");
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_3(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null
    final Map<Integer, String> values = new HashMap<Integer, String>() {};
    final int loadSize = 5;

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_11(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(0, "ABCDEFGHIJ");
            put(1, "abcdefghij");
            put(2, "0123456789");
            put(3, "!\"#$%&'()*");
            put(4, "ｦｧｨｩｪｫｬｭｮｯ");
            put(5, "ＡＢＣＤＥＦＧＨＩＪ");
            put(6, "ａｂｃｄｅｆｇｈｉｊ");
            put(7, "０１２３４５６７８９");
            put(8, "！”＃＄％＆’（）＊");
            put(9, "ぁあぃいぅうぇえぉお");
            put(10, "ァアィイゥウェエォオ");
            put(11, "亜唖娃阿哀愛挨姶逢葵");
            put(12, "弌丐丕个丱丶丼丿乂乖");
            put(13, "―ソ能表ЫⅨ噂浬欺圭");
            put(14, "①②③④⑤⑥⑦⑧⑨⑩");
            put(
                15,
                "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93");
            put(
                16,
                "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09");
          }
        };
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_12(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int loadSize = 8;
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, "abcdefghij");
            put(5, "!\"#$%&'()*");
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setBytes_13(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null
    final Map<Integer, String> values = new HashMap<Integer, String>() {};
    final int loadSize = 5;

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  void T_setDouble_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Double.MAX_VALUE,null,null,Double.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, -1 * Double.MAX_VALUE);
            put(5, Double.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  void T_setDouble_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Double.MAX_VALUE,null,null,Double.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, -1 * Double.MAX_VALUE);
            put(5, Double.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_floatColumnBinaryMaker")
  void T_setFloat_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Float.MAX_VALUE,null,null,Float.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Float> values =
        new HashMap<Integer, Float>() {
          {
            put(2, -1 * Float.MAX_VALUE);
            put(5, Float.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toFloatColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_floatColumnBinaryMaker")
  void T_setFloat_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Float.MAX_VALUE,null,null,Float.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Float> values =
        new HashMap<Integer, Float>() {
          {
            put(2, -1 * Float.MAX_VALUE);
            put(5, Float.MAX_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toFloatColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setInteger_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Integer.MAX_VALUE,null,null,Integer.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Integer> values =
        new HashMap<Integer, Integer>() {
          {
            put(2, Integer.MAX_VALUE);
            put(5, Integer.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toIntegerColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setInteger_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Integer.MAX_VALUE,null,null,Integer.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Integer> values =
        new HashMap<Integer, Integer>() {
          {
            put(2, Integer.MAX_VALUE);
            put(5, Integer.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toIntegerColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setLong_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Long.MAX_VALUE,null,null,Long.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Long> values =
        new HashMap<Integer, Long>() {
          {
            put(2, Long.MAX_VALUE);
            put(5, Long.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toLongColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setLong_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Long.MAX_VALUE,null,null,Long.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Long> values =
        new HashMap<Integer, Long>() {
          {
            put(2, Long.MAX_VALUE);
            put(5, Long.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toLongColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setShort_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Short.MAX_VALUE,null,null,Short.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Short> values =
        new HashMap<Integer, Short>() {
          {
            put(2, Short.MAX_VALUE);
            put(5, Short.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toShortColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  void T_setShort_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Short.MAX_VALUE,null,null,Short.MIN_VALUE,null,null
    final int loadSize = 8;
    final Map<Integer, Short> values =
        new HashMap<Integer, Short>() {
          {
            put(2, Short.MAX_VALUE);
            put(5, Short.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toShortColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(0, "ABCDEFGHIJ");
            put(1, "abcdefghij");
            put(2, "0123456789");
            put(3, "!\"#$%&'()*");
            put(4, "ｦｧｨｩｪｫｬｭｮｯ");
            put(5, "ＡＢＣＤＥＦＧＨＩＪ");
            put(6, "ａｂｃｄｅｆｇｈｉｊ");
            put(7, "０１２３４５６７８９");
            put(8, "！”＃＄％＆’（）＊");
            put(9, "ぁあぃいぅうぇえぉお");
            put(10, "ァアィイゥウェエォオ");
            put(11, "亜唖娃阿哀愛挨姶逢葵");
            put(12, "弌丐丕个丱丶丼丿乂乖");
            put(13, "―ソ能表ЫⅨ噂浬欺圭");
            put(14, "①②③④⑤⑥⑦⑧⑨⑩");
            put(
                15,
                "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93");
            put(
                16,
                "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09");
          }
        };
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int loadSize = 8;
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, "abcdefghij");
            put(5, "!\"#$%&'()*");
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_3(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null
    final Map<Integer, String> values = new HashMap<Integer, String>() {};
    final int loadSize = 5;

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_11(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(0, "ABCDEFGHIJ");
            put(1, "abcdefghij");
            put(2, "0123456789");
            put(3, "!\"#$%&'()*");
            put(4, "ｦｧｨｩｪｫｬｭｮｯ");
            put(5, "ＡＢＣＤＥＦＧＨＩＪ");
            put(6, "ａｂｃｄｅｆｇｈｉｊ");
            put(7, "０１２３４５６７８９");
            put(8, "！”＃＄％＆’（）＊");
            put(9, "ぁあぃいぅうぇえぉお");
            put(10, "ァアィイゥウェエォオ");
            put(11, "亜唖娃阿哀愛挨姶逢葵");
            put(12, "弌丐丕个丱丶丼丿乂乖");
            put(13, "―ソ能表ЫⅨ噂浬欺圭");
            put(14, "①②③④⑤⑥⑦⑧⑨⑩");
            put(
                15,
                "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93");
            put(
                16,
                "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09");
          }
        };
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_12(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int loadSize = 8;
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(2, "abcdefghij");
            put(5, "!\"#$%&'()*");
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }

  @ParameterizedTest
  @MethodSource("D_binaryColumnBinaryMaker")
  void T_setString_13(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null
    final Map<Integer, String> values = new HashMap<Integer, String>() {};
    final int loadSize = 5;

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISequentialLoader<WritableColumnVector> loader =
        new SparkSequentialBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, loadSize);
  }
}
