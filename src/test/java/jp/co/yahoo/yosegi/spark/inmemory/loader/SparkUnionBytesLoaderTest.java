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
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spark.test.UnionColumnUtils;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkUnionBytesLoaderTest {
  public static <T> void assertBytes(
      final Map<Integer, T> values, final WritableColumnVector vector) {
    for (final Integer i : values.keySet()) {
      assertArrayEquals(
          String.valueOf(values.get(i)).getBytes(StandardCharsets.UTF_8), vector.getBinary(i));
    }
  }

  public static <T> void assertString(
      final Map<Integer, T> values, final WritableColumnVector vector) {
    for (final Integer i : values.keySet()) {
      assertEquals(UTF8String.fromString(String.valueOf(values.get(i))), vector.getUTF8String(i));
    }
  }

  public static void assertIsNullAt(
      final WritableColumnVector vector, final int loadSize, final Set<Integer> keys) {
    for (int i = 0; i < loadSize; i++) {
      if (keys.contains(i)) {
        assertFalse(vector.isNullAt(i));
      } else {
        assertTrue(vector.isNullAt(i));
      }
    }
  }

  @Test
  void T_isTargetColumnType() {
    // NOTE: test data
    // NOTE: expected
    final ColumnType[] columnTypes = {
      ColumnType.BOOLEAN,
      ColumnType.BYTE,
      ColumnType.BYTES,
      ColumnType.DOUBLE,
      ColumnType.FLOAT,
      ColumnType.INTEGER,
      ColumnType.LONG,
      ColumnType.SHORT,
      ColumnType.STRING
    };

    final int loadSize = 5;
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final SparkUnionBytesLoader loader = new SparkUnionBytesLoader(vector, loadSize);

    // NOTE: assert
    for (final ColumnType columnType : Utils.getColumnTypes()) {
      final boolean actual = loader.isTargetColumnType(columnType);
      if (Arrays.asList(columnTypes).contains(columnType)) {
        assertTrue(actual);
      } else {
        assertFalse(actual);
      }
    }
  }

  @Test
  void T_load_1() throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int loadSize = 18;
    final Set<Integer> keys = new HashSet<>();
    final Map<Integer, Boolean> boValues =
        new HashMap<Integer, Boolean>() {
          {
            put(0, true);
            put(1, false);
          }
        };
    keys.addAll(boValues.keySet());
    final Map<Integer, Byte> byValues =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(3, Byte.MIN_VALUE);
          }
        };
    keys.addAll(byValues.keySet());
    final Map<Integer, String> btValues =
        new HashMap<Integer, String>() {
          {
            put(4, "ABCDEFGHIJ");
            put(5, "!\"#$%&'()*");
          }
        };
    keys.addAll(btValues.keySet());
    final Map<Integer, Double> doValues =
        new HashMap<Integer, Double>() {
          {
            put(6, Double.MAX_VALUE);
            put(7, Double.MIN_VALUE);
          }
        };
    keys.addAll(doValues.keySet());
    final Map<Integer, Float> flValues =
        new HashMap<Integer, Float>() {
          {
            put(8, Float.MAX_VALUE);
            put(9, Float.MIN_VALUE);
          }
        };
    keys.addAll(flValues.keySet());
    final Map<Integer, Integer> inValues =
        new HashMap<Integer, Integer>() {
          {
            put(10, Integer.MAX_VALUE);
            put(11, Integer.MIN_VALUE);
          }
        };
    keys.addAll(inValues.keySet());
    final Map<Integer, Long> loValues =
        new HashMap<Integer, Long>() {
          {
            put(12, Long.MAX_VALUE);
            put(13, Long.MIN_VALUE);
          }
        };
    keys.addAll(loValues.keySet());
    final Map<Integer, Short> shValues =
        new HashMap<Integer, Short>() {
          {
            put(14, Short.MAX_VALUE);
            put(15, Short.MIN_VALUE);
          }
        };
    keys.addAll(shValues.keySet());
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(16, "ABCDEFGHIJ");
            put(17, "!\"#$%&'()*");
          }
        };
    keys.addAll(stValues.keySet());

    // NOTE: create ColumnBinary
    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BOOLEAN, boValues);
    unionColumnUtils.add(ColumnType.BYTE, byValues);
    unionColumnUtils.add(ColumnType.BYTES, btValues);
    unionColumnUtils.add(ColumnType.DOUBLE, doValues);
    unionColumnUtils.add(ColumnType.FLOAT, flValues);
    unionColumnUtils.add(ColumnType.INTEGER, inValues);
    unionColumnUtils.add(ColumnType.LONG, loValues);
    unionColumnUtils.add(ColumnType.SHORT, shValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(boValues, vector);
    assertBytes(byValues, vector);
    assertBytes(btValues, vector);
    assertBytes(doValues, vector);
    assertBytes(flValues, vector);
    assertBytes(inValues, vector);
    assertBytes(loValues, vector);
    assertBytes(shValues, vector);
    assertBytes(stValues, vector);
    assertIsNullAt(vector, loadSize, keys);
  }

  @Test
  void T_load_2() throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int loadSize = 20;
    final Set<Integer> keys = new HashSet<>();
    final Map<Integer, Byte> byValues =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(3, Byte.MIN_VALUE);
          }
        };
    keys.addAll(byValues.keySet());
    final Map<Integer, String> btValues =
        new HashMap<Integer, String>() {
          {
            put(4, "ABCDEFGHIJ");
            put(5, "!\"#$%&'()*");
          }
        };
    keys.addAll(btValues.keySet());
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(16, "ABCDEFGHIJ");
            put(17, "!\"#$%&'()*");
          }
        };
    keys.addAll(stValues.keySet());

    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BYTE, byValues);
    unionColumnUtils.add(ColumnType.BYTES, btValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBytes(byValues, vector);
    assertBytes(btValues, vector);
    assertBytes(stValues, vector);
    assertIsNullAt(vector, loadSize, keys);
  }

  @Test
  void T_load_11() throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int loadSize = 18;
    final Set<Integer> keys = new HashSet<>();
    final Map<Integer, Boolean> boValues =
        new HashMap<Integer, Boolean>() {
          {
            put(0, true);
            put(1, false);
          }
        };
    keys.addAll(boValues.keySet());
    final Map<Integer, Byte> byValues =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(3, Byte.MIN_VALUE);
          }
        };
    keys.addAll(byValues.keySet());
    final Map<Integer, String> btValues =
        new HashMap<Integer, String>() {
          {
            put(4, "ABCDEFGHIJ");
            put(5, "!\"#$%&'()*");
          }
        };
    keys.addAll(btValues.keySet());
    final Map<Integer, Double> doValues =
        new HashMap<Integer, Double>() {
          {
            put(6, Double.MAX_VALUE);
            put(7, Double.MIN_VALUE);
          }
        };
    keys.addAll(doValues.keySet());
    final Map<Integer, Float> flValues =
        new HashMap<Integer, Float>() {
          {
            put(8, Float.MAX_VALUE);
            put(9, Float.MIN_VALUE);
          }
        };
    keys.addAll(flValues.keySet());
    final Map<Integer, Integer> inValues =
        new HashMap<Integer, Integer>() {
          {
            put(10, Integer.MAX_VALUE);
            put(11, Integer.MIN_VALUE);
          }
        };
    keys.addAll(inValues.keySet());
    final Map<Integer, Long> loValues =
        new HashMap<Integer, Long>() {
          {
            put(12, Long.MAX_VALUE);
            put(13, Long.MIN_VALUE);
          }
        };
    keys.addAll(loValues.keySet());
    final Map<Integer, Short> shValues =
        new HashMap<Integer, Short>() {
          {
            put(14, Short.MAX_VALUE);
            put(15, Short.MIN_VALUE);
          }
        };
    keys.addAll(shValues.keySet());
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(16, "ABCDEFGHIJ");
            put(17, "!\"#$%&'()*");
          }
        };
    keys.addAll(stValues.keySet());

    // NOTE: create ColumnBinary
    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BOOLEAN, boValues);
    unionColumnUtils.add(ColumnType.BYTE, byValues);
    unionColumnUtils.add(ColumnType.BYTES, btValues);
    unionColumnUtils.add(ColumnType.DOUBLE, doValues);
    unionColumnUtils.add(ColumnType.FLOAT, flValues);
    unionColumnUtils.add(ColumnType.INTEGER, inValues);
    unionColumnUtils.add(ColumnType.LONG, loValues);
    unionColumnUtils.add(ColumnType.SHORT, shValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(boValues, vector);
    assertString(byValues, vector);
    assertString(btValues, vector);
    assertString(doValues, vector);
    assertString(flValues, vector);
    assertString(inValues, vector);
    assertString(loValues, vector);
    assertString(shValues, vector);
    assertString(stValues, vector);
    assertIsNullAt(vector, loadSize, keys);
  }

  @Test
  void T_load_12() throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int loadSize = 20;
    final Set<Integer> keys = new HashSet<>();
    final Map<Integer, Byte> byValues =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MAX_VALUE);
            put(3, Byte.MIN_VALUE);
          }
        };
    keys.addAll(byValues.keySet());
    final Map<Integer, String> btValues =
        new HashMap<Integer, String>() {
          {
            put(4, "ABCDEFGHIJ");
            put(5, "!\"#$%&'()*");
          }
        };
    keys.addAll(btValues.keySet());
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(16, "ABCDEFGHIJ");
            put(17, "!\"#$%&'()*");
          }
        };
    keys.addAll(stValues.keySet());

    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BYTE, byValues);
    unionColumnUtils.add(ColumnType.BYTES, btValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionBytesLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertString(byValues, vector);
    assertString(btValues, vector);
    assertString(stValues, vector);
    assertIsNullAt(vector, loadSize, keys);
  }
}
