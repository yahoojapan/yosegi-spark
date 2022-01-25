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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.MaxLengthBasedArrayColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IArrayLoader;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkArrayLoaderTest {
  public static Stream<Arguments> D_arrayColumnBinaryMaker() {
    return Stream.of(arguments(MaxLengthBasedArrayColumnBinaryMaker.class.getName()));
  }

  public IColumn toArrayColumn(final String resource) throws IOException {
    final JacksonMessageReader jsonReader = new JacksonMessageReader();
    final ArrayColumn column = new ArrayColumn("column");
    final InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    int index = 0;
    while (reader.ready()) {
      final String json = reader.readLine();
      column.add(ColumnType.ARRAY, jsonReader.create(json), index);
      index++;
    }
    return column;
  }

  public <T> List<List<T>> toValues(final String resource) throws IOException {
    final InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    final List<List<T>> values = new ArrayList<>();
    while (reader.ready()) {
      final String json = reader.readLine();
      final ObjectMapper objectMapper = new ObjectMapper();
      final List<T> value = objectMapper.readValue(json, new TypeReference<List<T>>() {});
      values.add(value);
    }
    return values;
  }

  public <T> void assertArray(
      final List<List<T>> values,
      final WritableColumnVector vector,
      final int loadSize,
      final DataType dataType) {
    for (int i = 0; i < loadSize; i++) {
      // NOTE: assert array size
      assertEquals(values.get(i).size(), vector.getArrayLength(i));
      // NOTE: assert value
      for (int j = 0; j < values.get(i).size(); j++) {
        final T value = values.get(i).get(j);
        if (value == null) {
          assertTrue(vector.getArray(i).isNullAt(j));
        } else if (value instanceof Boolean) {
          final boolean expected = ((Boolean) value).booleanValue();
          assertFalse(vector.getArray(i).isNullAt(j));
          assertEquals(expected, vector.getArray(i).getBoolean(j));
        } else if (value instanceof String) {
          if (dataType == DataTypes.BinaryType) {
            final byte[] expected = ((String) value).getBytes(StandardCharsets.UTF_8);
            assertFalse(vector.getArray(i).isNullAt(j));
            assertArrayEquals(expected, vector.getArray(i).getBinary(j));
          } else if (dataType == DataTypes.StringType) {
            final UTF8String expected = UTF8String.fromString((String) value);
            assertFalse(vector.getArray(i).isNullAt(j));
            assertEquals(expected, vector.getArray(i).getUTF8String(j));
          }
        } else if (value instanceof Double) {
          if (dataType == DataTypes.DoubleType) {
            final double expected = ((Double) value).doubleValue();
            assertFalse(vector.getArray(i).isNullAt(j));
            assertEquals(expected, vector.getArray(i).getDouble(j));
          } else if (dataType == DataTypes.FloatType) {
            final float expected = ((Double) value).floatValue();
            assertFalse(vector.getArray(i).isNullAt(j));
            assertEquals(expected, vector.getArray(i).getFloat(j));
          }
        } else if (value instanceof Long) {
          final Long expected = ((Long) value).longValue();
          assertFalse(vector.getArray(i).isNullAt(j));
          assertEquals(expected, vector.getArray(i).getLong(j));
        } else {
          if (dataType == DataTypes.ByteType) {
            final byte expected = ((Integer) value).byteValue();
            assertFalse(vector.getArray(i).isNullAt(j));
            assertEquals(expected, vector.getArray(i).getByte(j));
          } else if (dataType == DataTypes.IntegerType) {
            final int expected = ((Integer) value).intValue();
            assertFalse(vector.getArray(i).isNullAt(j));
            assertEquals(expected, vector.getArray(i).getInt(j));
          } else if (dataType == DataTypes.ShortType) {
            final short expected = ((Integer) value).shortValue();
            assertFalse(vector.getArray(i).isNullAt(j));
            assertEquals(expected, vector.getArray(i).getShort(j));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Boolean_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Boolean_1.txt";
    final List<List<Boolean>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.BooleanType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Byte_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Byte_1.txt";
    final List<List<Byte>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.ByteType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Bytes_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/String_1.txt";
    final List<List<String>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.BinaryType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Double_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Double_1.txt";
    final List<List<Double>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.DoubleType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Float_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Float_1.txt";
    final List<List<Float>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.FloatType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Integer_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Integer_1.txt";
    final List<List<Integer>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.IntegerType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Long_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Long_1.txt";
    final List<List<Long>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.LongType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
    System.out.println(Long.MIN_VALUE);
    System.out.println(Long.MAX_VALUE);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_Short_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/Short_1.txt";
    final List<List<Short>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.ShortType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }

  @ParameterizedTest
  @MethodSource("D_arrayColumnBinaryMaker")
  void T_load_String_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkArrayLoaderTest/String_1.txt";
    final List<List<String>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toArrayColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType elmDataType = DataTypes.StringType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IArrayLoader<WritableColumnVector> loader = new SparkArrayLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertArray(values, vector, loadSize, elmDataType);
  }
}
