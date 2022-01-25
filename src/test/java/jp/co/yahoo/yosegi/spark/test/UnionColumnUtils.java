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
package jp.co.yahoo.yosegi.spark.test;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpUnionColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.UnionColumn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UnionColumnUtils {
  private final Map<ColumnType, IColumn> columnMap;
  private final Map<ColumnType, Object> valuesMap;
  private final int loadSize;
  private final IColumnBinaryMaker binaryMaker;

  public UnionColumnUtils(final int loadSize) throws IOException {
    this.loadSize = loadSize;
    columnMap = new HashMap<>();
    valuesMap = new HashMap<>();
    binaryMaker = FindColumnBinaryMaker.get(DumpUnionColumnBinaryMaker.class.getName());
  }

  public IColumnBinaryMaker getBinaryMaker() {
    return binaryMaker;
  }

  @SuppressWarnings("unchecked")
  public ColumnBinary createColumnBinary() throws IOException {
    final IColumn column = new UnionColumn("column", columnMap);
    for (int i = 0; i < loadSize; i++) {
      for (final ColumnType columnType : columnMap.keySet()) {
        final Map<Integer, Object> values = (Map<Integer, Object>) valuesMap.get(columnType);
        final IColumn childColumn = columnMap.get(columnType);
        if (values.containsKey(i)) {
          column.addCell(columnType, childColumn.get(i), i);
        }
      }
    }
    return Utils.getColumnBinary(binaryMaker, column, null, null, null);
  }

  @SuppressWarnings("unchecked")
  public void add(final ColumnType columnType, final Object values) throws IOException {
    valuesMap.put(columnType, values);
    switch (columnType) {
      case STRING:
        columnMap.put(columnType, Utils.toStringColumn((Map<Integer, String>) values, loadSize));
        return;
      case BYTES:
        columnMap.put(columnType, Utils.toBytesColumn((Map<Integer, String>) values, loadSize));
        return;
      case BOOLEAN:
        columnMap.put(columnType, Utils.toBooleanColumn((Map<Integer, Boolean>) values, loadSize));
        return;
      case LONG:
        columnMap.put(columnType, Utils.toLongColumn((Map<Integer, Long>) values, loadSize));
        return;
      case INTEGER:
        columnMap.put(columnType, Utils.toIntegerColumn((Map<Integer, Integer>) values, loadSize));
        return;
      case SHORT:
        columnMap.put(columnType, Utils.toShortColumn((Map<Integer, Short>) values, loadSize));
        return;
      case BYTE:
        columnMap.put(columnType, Utils.toByteColumn((Map<Integer, Byte>) values, loadSize));
        return;
      case DOUBLE:
        columnMap.put(columnType, Utils.toDoubleColumn((Map<Integer, Double>) values, loadSize));
        return;
      case FLOAT:
        columnMap.put(columnType, Utils.toFloatColumn((Map<Integer, Float>) values, loadSize));
        return;
      default:
        throw new IOException("unsupported ColumnType");
    }
  }
}
