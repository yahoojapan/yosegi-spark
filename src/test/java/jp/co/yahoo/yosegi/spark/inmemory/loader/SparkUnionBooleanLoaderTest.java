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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkUnionBooleanLoaderTest {
  public void assertBoolean(
      final Boolean[] expected, final WritableColumnVector vector, final int loadSize) {
    for (int i = 0; i < loadSize; i++) {
      if (expected[i] == null) {
        assertEquals(true, vector.isNullAt(i));
      } else {
        assertEquals(expected[i], vector.getBoolean(i));
      }
    }
  }

  @Test
  void T_isTargetColumnType() {
    // NOTE: test data
    // NOTE: expected
    final ColumnType[] columnTypes = {ColumnType.BOOLEAN, ColumnType.BYTES, ColumnType.STRING};

    final int loadSize = 5;
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final SparkUnionBooleanLoader loader = new SparkUnionBooleanLoader(vector, loadSize);

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
    // 0,1:BOOLEAN, 2,3:STRING, 4,5:LONG
    final int loadSize = 6;
    final Map<Integer, Boolean> boValues =
        new HashMap<Integer, Boolean>() {
          {
            put(0, true);
            put(1, false);
          }
        };
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(2, "true");
            put(3, "false");
          }
        };
    final Map<Integer, Long> loValues =
        new HashMap<Integer, Long>() {
          {
            put(4, 4L);
            put(5, 5L);
          }
        };
    // NOTE: expected
    final Boolean[] expected = new Boolean[] {true, false, true, false, null, null};

    // NOTE: create ColumnBinary
    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BOOLEAN, boValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    unionColumnUtils.add(ColumnType.LONG, loValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }

  @Test
  void T_load_2() throws IOException {
    // NOTE: test data
    // 2,6:BOOLEAN, 4,10:STRING, 8,12:LONG
    final int loadSize = 13;
    final Map<Integer, Boolean> boValues =
        new HashMap<Integer, Boolean>() {
          {
            put(2, true);
            put(6, false);
          }
        };
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(4, "true");
            put(10, "false");
          }
        };
    final Map<Integer, Long> loValues =
        new HashMap<Integer, Long>() {
          {
            put(8, 4L);
            put(12, 5L);
          }
        };
    // NOTE: expected
    final Boolean[] expected =
        new Boolean[] {
          null, null, true, null, true, null, false, null, null, null, false, null, null, null
        };

    // NOTE: create ColumnBinary
    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BOOLEAN, boValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    unionColumnUtils.add(ColumnType.LONG, loValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionBooleanLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertBoolean(expected, vector, loadSize);
  }
}
