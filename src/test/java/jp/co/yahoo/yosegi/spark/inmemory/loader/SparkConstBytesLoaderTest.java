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
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkConstBytesLoaderTest {
  public void assertBytes(
      final String expected, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (expected == null) {
        assertTrue(vector.isNullAt(i));
      } else {
        assertFalse(vector.isNullAt(i));
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), vector.getBinary(i));
      }
    }
  }

  public void assertString(
      final String expected, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (expected == null) {
        assertTrue(vector.isNullAt(i));
      } else {
        assertFalse(vector.isNullAt(i));
        assertEquals(UTF8String.fromString(expected), vector.getUTF8String(i));
      }
    }
  }

  @Test
  void T_setConstFromBoolean_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BooleanObj value = new BooleanObj(true);
    // NOTE: expected
    final String expected = String.valueOf(true);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBoolean_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final BooleanObj value = new BooleanObj(true);
    // NOTE: expected
    final String expected = String.valueOf(true);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromByte_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ByteObj value = new ByteObj(Byte.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Byte.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromByte_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ByteObj value = new ByteObj(Byte.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Byte.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromShort_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ShortObj value = new ShortObj(Short.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Short.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromShort_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final ShortObj value = new ShortObj(Short.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Short.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromInteger_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final IntegerObj value = new IntegerObj(Integer.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Integer.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromInteger_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final IntegerObj value = new IntegerObj(Integer.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Integer.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromLong_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final LongObj value = new LongObj(Long.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Long.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromLong_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final LongObj value = new LongObj(Long.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Long.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromFloat_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final FloatObj value = new FloatObj(Float.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Float.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromFloat_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final FloatObj value = new FloatObj(Float.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Float.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromDouble_1() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final DoubleObj value = new DoubleObj(Double.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Double.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromDouble_2() throws IOException {
    // NOTE: test data
    final int loadSize = 5;
    final DoubleObj value = new DoubleObj(Double.MIN_VALUE);
    // NOTE: expected
    final String expected = String.valueOf(Double.MIN_VALUE);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_1() throws IOException {
    // NOTE: expected
    final String expected =
        "ABCDEFGHIJ"
            + "abcdefghij"
            + "0123456789"
            + "!\"#$%&'()*"
            + "ｦｧｨｩｪｫｬｭｮｯ"
            + "ＡＢＣＤＥＦＧＨＩＪ"
            + "ａｂｃｄｅｆｇｈｉｊ"
            + "０１２３４５６７８９"
            + "！”＃＄％＆’（）＊"
            + "ぁあぃいぅうぇえぉお"
            + "ァアィイゥウェエォオ"
            + "亜唖娃阿哀愛挨姶逢葵"
            + "弌丐丕个丱丶丼丿乂乖"
            + "―ソ能表ЫⅨ噂浬欺圭"
            + "①②③④⑤⑥⑦⑧⑨⑩"
            + "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93"
            + "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09";
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value = new BytesObj(expected.getBytes(StandardCharsets.UTF_8));

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromBytes_2() throws IOException {
    // NOTE: expected
    final String expected =
        "ABCDEFGHIJ"
            + "abcdefghij"
            + "0123456789"
            + "!\"#$%&'()*"
            + "ｦｧｨｩｪｫｬｭｮｯ"
            + "ＡＢＣＤＥＦＧＨＩＪ"
            + "ａｂｃｄｅｆｇｈｉｊ"
            + "０１２３４５６７８９"
            + "！”＃＄％＆’（）＊"
            + "ぁあぃいぅうぇえぉお"
            + "ァアィイゥウェエォオ"
            + "亜唖娃阿哀愛挨姶逢葵"
            + "弌丐丕个丱丶丼丿乂乖"
            + "―ソ能表ЫⅨ噂浬欺圭"
            + "①②③④⑤⑥⑦⑧⑨⑩"
            + "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93"
            + "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09";
    // NOTE: test data
    final int loadSize = 5;
    final BytesObj value = new BytesObj(expected.getBytes(StandardCharsets.UTF_8));

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_1() throws IOException {
    // NOTE: expected
    final String expected =
        "ABCDEFGHIJ"
            + "abcdefghij"
            + "0123456789"
            + "!\"#$%&'()*"
            + "ｦｧｨｩｪｫｬｭｮｯ"
            + "ＡＢＣＤＥＦＧＨＩＪ"
            + "ａｂｃｄｅｆｇｈｉｊ"
            + "０１２３４５６７８９"
            + "！”＃＄％＆’（）＊"
            + "ぁあぃいぅうぇえぉお"
            + "ァアィイゥウェエォオ"
            + "亜唖娃阿哀愛挨姶逢葵"
            + "弌丐丕个丱丶丼丿乂乖"
            + "―ソ能表ЫⅨ噂浬欺圭"
            + "①②③④⑤⑥⑦⑧⑨⑩"
            + "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93"
            + "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09";
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj(expected);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromString_2() throws IOException {
    // NOTE: expected
    final String expected =
        "ABCDEFGHIJ"
            + "abcdefghij"
            + "0123456789"
            + "!\"#$%&'()*"
            + "ｦｧｨｩｪｫｬｭｮｯ"
            + "ＡＢＣＤＥＦＧＨＩＪ"
            + "ａｂｃｄｅｆｇｈｉｊ"
            + "０１２３４５６７８９"
            + "！”＃＄％＆’（）＊"
            + "ぁあぃいぅうぇえぉお"
            + "ァアィイゥウェエォオ"
            + "亜唖娃阿哀愛挨姶逢葵"
            + "弌丐丕个丱丶丼丿乂乖"
            + "―ソ能表ЫⅨ噂浬欺圭"
            + "①②③④⑤⑥⑦⑧⑨⑩"
            + "\uD83C\uDC04\uD83C\uDCCF\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91\uD83C\uDD92\uD83C\uDD93"
            + "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02\uD83D\uDE03\uD83D\uDE04\uD83D\uDE05\uD83D\uDE06\uD83D\uDE07\uD83D\uDE08\uD83D\uDE09";
    // NOTE: test data
    final int loadSize = 5;
    final StringObj value = new StringObj(expected);

    // NOTE: create ColumnBinary
    final ColumnBinary columnBinary =
        ConstantColumnBinaryMaker.createColumnBinary(value, "column", loadSize);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(ConstantColumnBinaryMaker.class.getName());

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(expected, vector, loadSize);
  }

  @Test
  void T_setConstFromNull_1() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    loader.setConstFromNull();

    // NOTE: assert
    assertBytes(null, vector, loadSize);
  }

  @Test
  void T_setConstFromNull_2() throws IOException {
    // NOTE: load
    final int loadSize = 5;
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IConstLoader<WritableColumnVector> loader = new SparkConstBytesLoader(vector, loadSize);
    loader.setConstFromNull();

    // NOTE: assert
    assertBytes(null, vector, loadSize);
  }
}
