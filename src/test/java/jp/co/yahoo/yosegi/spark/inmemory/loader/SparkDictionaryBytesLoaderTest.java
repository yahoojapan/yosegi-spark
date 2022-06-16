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
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
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

class SparkDictionaryBytesLoaderTest {
  public static Stream<Arguments> D_columnBinaryMaker() {
    return Stream.of(arguments(OptimizedNullArrayDumpBytesColumnBinaryMaker.class.getName()));
  }

  public static <T> void assertBytes(
      final Map<Integer, T> values, final WritableColumnVector vector, final int[] repetitions) {
    int rowId = 0;
    for (int i = 0; i < repetitions.length; i++) {
      if (values.containsKey(i)) {
        for (int j = 0; j < repetitions[i]; j++) {
          assertFalse(vector.isNullAt(rowId));
          assertArrayEquals(
              String.valueOf(values.get(i)).getBytes(StandardCharsets.UTF_8),
              vector.getBinary(rowId));
          rowId++;
        }
        //
      } else {
        for (int j = 0; j < repetitions[i]; j++) {
          assertTrue(vector.isNullAt(rowId));
          rowId++;
        }
      }
    }
  }

  public static <T> void assertString(
      final Map<Integer, T> values, final WritableColumnVector vector, final int[] repetitions) {
    int rowId = 0;
    for (int i = 0; i < repetitions.length; i++) {
      if (values.containsKey(i)) {
        for (int j = 0; j < repetitions[i]; j++) {
          assertFalse(vector.isNullAt(rowId));
          assertEquals(
              UTF8String.fromString(String.valueOf(values.get(i))), vector.getUTF8String(rowId));
          rowId++;
        }
        //
      } else {
        for (int j = 0; j < repetitions[i]; j++) {
          assertTrue(vector.isNullAt(rowId));
          rowId++;
        }
      }
    }
  }

  @Test
  void createDictionary() {}

  @Test
  void setDictionaryIndex() {}

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBooleanToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,null,null,false,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
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
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBooleanToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,true,null,null,false,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
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
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setByteToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Byte.MAX_VALUE,null,null,Byte.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setByteToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Byte.MAX_VALUE,null,null,Byte.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_3(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null,null,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, String> values = new HashMap<Integer, String>() {};

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_4(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null*2,v*3,null*2,null,v*2,null,null*2
    final int[] repetitions = new int[] {1, 2, 3, 2, 1, 2, 1, 2};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_11(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_12(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_13(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null,null,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, String> values = new HashMap<Integer, String>() {};

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toBytesColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setBytesToDic_14(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null*2,v*3,null*2,null,v*2,null,null*2
    final int[] repetitions = new int[] {1, 2, 3, 2, 1, 2, 1, 2};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setDoubleToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Double.MAX_VALUE,null,null,Double.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, Double.MAX_VALUE);
            put(5, Double.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setDoubleToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Double.MAX_VALUE,null,null,Double.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(2, Double.MAX_VALUE);
            put(5, Double.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toDoubleColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  // FIXME: float does not have a dictionary load type.
  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setFloatToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Float.MAX_VALUE,null,null,Float.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, Float> values =
        new HashMap<Integer, Float>() {
          {
            put(2, Float.MAX_VALUE);
            put(5, Float.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toFloatColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  // FIXME: float does not have a dictionary load type.
  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setFloatToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Float.MAX_VALUE,null,null,Float.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, Float> values =
        new HashMap<Integer, Float>() {
          {
            put(2, Float.MAX_VALUE);
            put(5, Float.MIN_VALUE);
          }
        };

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toFloatColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setIntegerToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Integer.MAX_VALUE,null,null,Integer.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setIntegerToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Integer.MAX_VALUE,null,null,Integer.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setLongToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Long.MAX_VALUE,null,null,Long.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setLongToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Long.MAX_VALUE,null,null,Long.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setShortToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Short.MAX_VALUE,null,null,Short.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setShortToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,Short.MAX_VALUE,null,null,Short.MIN_VALUE,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_2(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_3(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null,null,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, String> values = new HashMap<Integer, String>() {};

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_4(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null*2,v*3,null*2,null,v*2,null,null*2
    final int[] repetitions = new int[] {1, 2, 3, 2, 1, 2, 1, 2};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_11(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_12(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,v,null,null,v,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_13(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,null,null,null,null,null,null
    final int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
    final int loadSize = Utils.getLoadSize(repetitions);
    final Map<Integer, String> values = new HashMap<Integer, String>() {};

    // NOTE: create ColumnBinary
    final IColumn column = Utils.toStringColumn(values, loadSize);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @ParameterizedTest
  @MethodSource("D_columnBinaryMaker")
  void T_setStringToDic_14(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null*2,v*3,null*2,null,v*2,null,null*2
    final int[] repetitions = new int[] {1, 2, 3, 2, 1, 2, 1, 2};
    final int loadSize = Utils.getLoadSize(repetitions);
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
    columnBinary.setRepetitions(repetitions, loadSize);

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IDictionaryLoader<WritableColumnVector> loader =
        new SparkDictionaryBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(values, vector, repetitions);
  }

  @Test
  void T_setNullToDic_1() {
    // FIXME:
  }
}
