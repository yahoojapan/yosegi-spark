/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.inmemory;

import java.util.Objects;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.NullMemoryAllocator;


public final class SparkMemoryAllocatorFactory {
  private static final Map<Class, MemoryAllocatorFactory> dispatch = new HashMap<>();
  static {
    dispatch.put(ArrayType.class,   (v, rc) -> new SparkArrayMemoryAllocator(v, rc));
    dispatch.put(StructType.class,  (v, rc) -> new SparkStructMemoryAllocator(v, rc, (StructType)v.dataType()));
    dispatch.put(DataTypes.StringType.getClass(),  (v, rc) -> new SparkBytesMemoryAllocator(v, rc));
    dispatch.put(DataTypes.BinaryType.getClass(),  (v, rc) -> new SparkBytesMemoryAllocator(v, rc));
    dispatch.put(DataTypes.BooleanType.getClass(), (v, rc) -> new SparkBooleanMemoryAllocator(v, rc));
    dispatch.put(DataTypes.ByteType.getClass(),    (v, rc) -> new SparkByteMemoryAllocator(v, rc));
    dispatch.put(DataTypes.ShortType.getClass(),   (v, rc) -> new SparkShortMemoryAllocator(v, rc));
    dispatch.put(DataTypes.IntegerType.getClass(), (v, rc) -> new SparkIntegerMemoryAllocator(v, rc));
    dispatch.put(DataTypes.LongType.getClass(),    (v, rc) -> new SparkLongMemoryAllocator(v, rc));
    dispatch.put(DataTypes.FloatType.getClass(),   (v, rc) -> new SparkFloatMemoryAllocator(v, rc));
    dispatch.put(DataTypes.DoubleType.getClass(),  (v, rc) -> new SparkDoubleMemoryAllocator(v, rc));
    dispatch.put(DataTypes.TimestampType.getClass(),  (v, rc) -> new SparkLongMemoryAllocator(v, rc));
    dispatch.put(DecimalType.class,  (v, rc) -> new SparkDecimalMemoryAllocator(v, rc));

    dispatch.put(MapType.class, (vector, rowCount) -> {
      if (!(vector.getChildColumn(0).dataType() instanceof StringType)) {
        throw new UnsupportedOperationException(makeErrorMessage(vector) + ". Map key type is string only.");
      }
      return new SparkMapMemoryAllocator(vector, rowCount);
    });
  }

  private SparkMemoryAllocatorFactory() {}

  public static IMemoryAllocator get(final ColumnVector vector, final int rowCount) {
    if ( vector == null ) {
      return NullMemoryAllocator.INSTANCE;
    }
    MemoryAllocatorFactory factory = dispatch.get(vector.dataType().getClass());
    if (Objects.isNull(factory)) throw new UnsupportedOperationException(makeErrorMessage(vector));
    return factory.get(vector, rowCount);
  }

  private static String makeErrorMessage(final ColumnVector vector) {
    return "Unsupported datatype : " + vector.dataType().toString();
  }

  @FunctionalInterface
  private static interface MemoryAllocatorFactory {
    IMemoryAllocator get(final ColumnVector vector, final int rowCount) throws UnsupportedOperationException;
  }
}

