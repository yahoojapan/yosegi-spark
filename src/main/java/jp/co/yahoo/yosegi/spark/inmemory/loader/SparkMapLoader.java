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
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SparkMapLoader implements ISpreadLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private final boolean compositeDataType;

  private final List<WritableColumnVector> childVectors;
  private final List<String> childKeys;
  private int numNulls;

  public SparkMapLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    numNulls = 0;
    compositeDataType = isCompositeDataType(vector.getChild(1).dataType().getClass());
    childVectors = new ArrayList<>();
    childKeys = new ArrayList<>();
  }

  private boolean isCompositeDataType(final Class klass) {
    if (klass == MapType.class || klass == ArrayType.class || klass == StructType.class) {
      return true;
    }
    return false;
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
    if (compositeDataType) {
      return;
    }
    // NOTE: set key-value pairs without null value
    final int capacity = loadSize * childVectors.size() - numNulls;
    vector.getChild(0).reset();
    vector.getChild(0).reserve(capacity);
    vector.getChild(1).reset();
    vector.getChild(1).reserve(capacity);
    int offset = 0;
    int count = 0;
    for (int i = 0; i < loadSize; i++) {
      for (int j = 0; j < childVectors.size(); j++) {
        if (childVectors.get(j).isNullAt(i)) {
          continue;
        }
        vector.getChild(0).putByteArray(offset, childKeys.get(j).getBytes(StandardCharsets.UTF_8));
        final Class klass = vector.getChild(1).dataType().getClass();
        if (klass == DataTypes.StringType.getClass() || klass == DataTypes.BinaryType.getClass()) {
          vector.getChild(1).putByteArray(offset, childVectors.get(j).getBinary(i));
        } else if (klass == DataTypes.BooleanType.getClass()) {
          vector.getChild(1).putBoolean(offset, childVectors.get(j).getBoolean(i));
        } else if (klass == DataTypes.LongType.getClass()) {
          vector.getChild(1).putLong(offset, childVectors.get(j).getLong(i));
        } else if (klass == DataTypes.IntegerType.getClass()) {
          vector.getChild(1).putInt(offset, childVectors.get(j).getInt(i));
        } else if (klass == DataTypes.ShortType.getClass()) {
          vector.getChild(1).putShort(offset, childVectors.get(j).getShort(i));
        } else if (klass == DataTypes.ByteType.getClass()) {
          vector.getChild(1).putByte(offset, childVectors.get(j).getByte(i));
        } else if (klass == DataTypes.DoubleType.getClass()) {
          vector.getChild(1).putDouble(offset, childVectors.get(j).getDouble(i));
        } else if (klass == DataTypes.FloatType.getClass()) {
          vector.getChild(1).putFloat(offset, childVectors.get(j).getFloat(i));
        } else if (klass == DecimalType.class) {
          final DecimalType dt = ((DecimalType) vector.getChild(1).dataType());
          vector
              .getChild(1)
              .putDecimal(
                  offset,
                  childVectors.get(j).getDecimal(i, dt.precision(), dt.scale()),
                  dt.precision());
        } else if (klass == DataTypes.TimestampType.getClass()) {
          vector.getChild(1).putLong(offset, childVectors.get(j).getLong(i));
        } else {
          vector.getChild(1).putNull(offset);
        }
        offset++;
        count++;
      }
      vector.putArray(i, offset - count, count);
      count = 0;
    }
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int loadSize) throws IOException {
    if (compositeDataType) {
      // FIXME:
    } else {
      final WritableColumnVector childVector =
          new OnHeapColumnVector(loadSize, vector.getChild(1).dataType());
      SparkLoaderFactoryUtil.createLoaderFactory(childVector).create(columnBinary, loadSize);
      childVectors.add(childVector);
      childKeys.add(columnBinary.columnName);
      numNulls += childVector.numNulls();
    }
  }
}
