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
import org.apache.spark.sql.types.StructField;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkStructLoaderTest {
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

  public void assertStruct(
      final List<Map<String, Object>> values,
      final WritableColumnVector vector,
      final int loadSize,
      final List<StructField> fields) {
    for (int i = 0; i < loadSize; i++) {
      for (int j = 0; j < fields.size(); j++) {
        final StructField field = fields.get(j);
        final String name = field.name();
        final DataType dataType = field.dataType();
        if (values.get(i).containsKey(name)) {
          if (dataType == DataTypes.BooleanType) {
            final Boolean expected = (Boolean) values.get(i).get(name);
            if (expected == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              assertEquals(expected, vector.getChild(j).getBoolean(i));
            }
          } else if (dataType == DataTypes.BinaryType) {
            final String expected = (String) values.get(i).get(name);
            if (expected == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              assertFalse(vector.getChild(j).isNullAt(i));
              assertArrayEquals(
                  expected.getBytes(StandardCharsets.UTF_8), vector.getChild(j).getBinary(i));
            }
          } else if (dataType == DataTypes.StringType) {
            final String expected = (String) values.get(i).get(name);
            if (expected == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(UTF8String.fromString(expected), vector.getChild(j).getUTF8String(i));
            }
          } else if (dataType == DataTypes.ByteType) {
            final Integer value = (Integer) values.get(i).get(name);
            if (value == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              final Byte expected = value.byteValue();
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(expected, vector.getChild(j).getByte(i));
            }
          } else if (dataType == DataTypes.DoubleType) {
            final Double expected = (Double) values.get(i).get(name);
            if (expected == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(expected, vector.getChild(j).getDouble(i));
            }
          } else if (dataType == DataTypes.FloatType) {
            final Double value = (Double) values.get(i).get(name);
            if (value == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              final Float expected = value.floatValue();
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(expected, vector.getChild(j).getFloat(i));
            }
          } else if (dataType == DataTypes.IntegerType) {
            final Integer value = (Integer) values.get(i).get(name);
            if (value == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              final Integer expected = value.intValue();
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(expected, vector.getChild(j).getInt(i));
            }
          } else if (dataType == DataTypes.LongType) {
            final Long expected = (Long) values.get(i).get(name);
            if (expected == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(expected, vector.getChild(j).getLong(i));
            }
          } else if (dataType == DataTypes.ShortType) {
            final Integer value = (Integer) values.get(i).get(name);
            if (value == null) {
              assertTrue(vector.getChild(j).isNullAt(i));
            } else {
              final Short expected = value.shortValue();
              assertFalse(vector.getChild(j).isNullAt(i));
              assertEquals(expected, vector.getChild(j).getShort(i));
            }
          } else {
            // FIXME: invalid DataType
            assertTrue(false);
          }
        } else {
          assertTrue(vector.getChild(j).isNullAt(i));
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_load_Struct_1(final String binaryMakerClassName) throws IOException {
    // NOTE: test data
    // NOTE: expected
    final String resource = "SparkStructLoaderTest/Struct_1.txt";
    final List<Map<String, Object>> values = toValues(resource);
    final int loadSize = values.size();

    // NOTE: create ColumnBinary
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker = FindColumnBinaryMaker.get(binaryMakerClassName);
    final ColumnBinary columnBinary = Utils.getColumnBinary(binaryMaker, column, null, null, null);

    // NOTE: load
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("bi", DataTypes.BinaryType, true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final DataType dataType = DataTypes.createStructType(fields);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ISpreadLoader<WritableColumnVector> loader = new SparkStructLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertStruct(values, vector, loadSize, fields);
  }
}
