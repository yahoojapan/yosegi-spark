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
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnarMap;
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
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkMapLoaderTest {
  public static Stream<Arguments> D_spreadColumnBinaryMaker() {
    return Stream.of(arguments(DumpSpreadColumnBinaryMaker.class.getName()));
  }

  public IColumn toSpreadColumn(final String resource) throws IOException {
    final JacksonMessageReader jsonReader = new JacksonMessageReader();
    final SpreadColumn column = new SpreadColumn("column");
    final InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    int index = 0;
    while (reader.ready()) {
      final String json = reader.readLine();
      column.add(ColumnType.SPREAD, jsonReader.create(json), index);
      index++;
    }
    return column;
  }

  public List<Map<String, Object>> toValues(final String resource) throws IOException {
    final InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    final List<Map<String, Object>> values = new ArrayList<>();
    while (reader.ready()) {
      final String json = reader.readLine();
      final ObjectMapper objectMapper = new ObjectMapper();
      final Map<String, Object> value =
          objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
      values.add(value);
    }
    return values;
  }

  public void assertMap(
      final List<Map<String, Object>> values,
      final WritableColumnVector vector,
      final int loadSize,
      final DataType valueType) {
    for (int i = 0; i < loadSize; i++) {
      int size = 0;
      final ColumnarMap cm = vector.getMap(i);
      for (final Object key : values.get(i).keySet().toArray()) {
        final Object value = values.get(i).get(key);
        boolean exists = false;
        size += (value == null) ? 0 : 1;
        for (int j = 0; j < cm.numElements(); j++) {
          if (!cm.keyArray().isNullAt(j)) {
            if (cm.keyArray().getUTF8String(j).toString().equals(key)) {
              if (cm.valueArray().isNullAt(j)) {
                System.out.println("NG");
              } else {
                if (value == null) {
                  assertTrue(cm.valueArray().isNullAt(j));
                } else if (valueType == DataTypes.BooleanType) {
                  final Boolean expected = (Boolean) value;
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected, cm.valueArray().getBoolean(j));
                } else if (valueType == DataTypes.BinaryType) {
                  final String expected = (String) value;
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertArrayEquals(
                      expected.getBytes(StandardCharsets.UTF_8), cm.valueArray().getBinary(j));
                } else if (valueType == DataTypes.StringType) {
                  final String expected = (String) value;
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(UTF8String.fromString(expected), cm.valueArray().getUTF8String(j));
                } else if (valueType == DataTypes.ByteType) {
                  final Integer expected = (Integer) value;
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected.byteValue(), cm.valueArray().getByte(j));
                } else if (valueType == DataTypes.DoubleType) {
                  final Double expected =
                      (value instanceof Double) ? (Double) value : ((Integer) value).doubleValue();
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected.doubleValue(), cm.valueArray().getDouble(j));
                } else if (valueType == DataTypes.FloatType) {
                  final Double expected =
                      (value instanceof Double) ? (Double) value : ((Integer) value).doubleValue();
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected.floatValue(), cm.valueArray().getFloat(j));
                } else if (valueType == DataTypes.IntegerType) {
                  final Integer expected = (Integer) value;
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected.intValue(), cm.valueArray().getInt(j));
                } else if (valueType == DataTypes.LongType) {
                  final Long expected =
                      (value instanceof Long) ? (Long) value : ((Integer) value).longValue();
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected.longValue(), cm.valueArray().getLong(j));
                } else if (valueType == DataTypes.ShortType) {
                  final Integer expected = (Integer) value;
                  assertFalse(cm.valueArray().isNullAt(j));
                  assertEquals(expected.shortValue(), cm.valueArray().getShort(j));
                } else {
                  // FIXME: invald DataType
                  assertTrue(false);
                }
              }
              exists = true;
              break;
            }
          }
        }
      }
      // NOTE: assert key size
      assertEquals(size, cm.numElements());
    }
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Boolean_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Boolean_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.BooleanType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Byte_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Byte_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.ByteType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Bytes_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/String_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.BinaryType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Double_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Double_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.DoubleType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Float_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Float_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.FloatType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Integer_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Integer_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.IntegerType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Long_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Long_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.LongType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Short_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/Short_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.ShortType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_String_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkMapLoaderTest/String_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final DataType valueType = DataTypes.StringType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkMapLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertMap(values, vector, loadSize, valueType);
  }
}
