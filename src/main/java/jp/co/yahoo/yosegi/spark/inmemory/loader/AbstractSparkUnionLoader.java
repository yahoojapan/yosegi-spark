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
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSparkUnionLoader implements IUnionLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;
  protected final Map<ColumnType, WritableColumnVector> childVectors;

  public AbstractSparkUnionLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    childVectors = new HashMap<>();
  }

  protected abstract void setValue(final int index, final WritableColumnVector childVector);

  protected abstract boolean isTargetColumnType(final ColumnType columnType);
  /*{
    switch (columnType) {
      case UNION:
      case ARRAY:
      case SPREAD:
      case BOOLEAN:
      case BYTE:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INTEGER:
      case LONG:
      case SHORT:
      case STRING:
      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      case MAP:
      case STRUCT:
      case UNKNOWN:
      default:
        return false;
    }
  }*/

  protected DataType columnDataType() {
    return vector.dataType();
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    // FIXME:
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void setIndexAndColumnType(final int index, final ColumnType columnType)
      throws IOException {
    if (!childVectors.containsKey(columnType)) {
      vector.putNull(index);
      return;
    }
    final WritableColumnVector childVector = childVectors.get(columnType);
    if (childVector.isNullAt(index)) {
      vector.putNull(index);
    } else {
      setValue(index, childVector);
    }
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
      throws IOException {
    if (isTargetColumnType(columnBinary.columnType)) {
      final WritableColumnVector childVector = new OnHeapColumnVector(loadSize, columnDataType());
      SparkLoaderFactoryUtil.createLoaderFactory(childVector).create(columnBinary, childLoadSize);
      childVectors.put(columnBinary.columnType, childVector);
    }
  }
}
