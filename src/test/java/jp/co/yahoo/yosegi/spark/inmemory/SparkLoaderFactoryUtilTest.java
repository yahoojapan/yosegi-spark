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
package jp.co.yahoo.yosegi.spark.inmemory;

import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkArrayLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkBooleanLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkByteLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkBytesLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkDecimalLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkDoubleLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkFloatLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkIntegerLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkLongLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkMapLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkShortLoaderFactory;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkLoaderFactoryUtilTest {
  @Test
  void T_createLoaderFactory_ArrayType() {
    final int loadSize = 5;
    final DataType elmDataType = DataTypes.BooleanType;
    final DataType dataType = DataTypes.createArrayType(elmDataType);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkArrayLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_BinaryType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.BinaryType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkBytesLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_StringType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.StringType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkBytesLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_BooleanType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.BooleanType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkBooleanLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_ByteType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.ByteType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkByteLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_ShortType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.ShortType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkShortLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_IntegerType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.IntegerType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkIntegerLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_LongType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.LongType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkLongLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_FloatType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkFloatLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_DoubleType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.DoubleType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkDoubleLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_TimestampType() {
    final int loadSize = 5;
    final DataType dataType = DataTypes.TimestampType;
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkLongLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_DecimalType() {
    final int loadSize = 5;
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkDecimalLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_MapType_1() {
    final int loadSize = 5;
    final DataType valueType = DataTypes.BooleanType;
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoaderFactory<WritableColumnVector> loader =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loader instanceof SparkMapLoaderFactory);
  }

  @Test
  void T_createLoaderFactory_MapType_2() {
    final int loadSize = 5;
    final DataType elmDataType = DataTypes.BooleanType;
    final DataType valueType = DataTypes.createArrayType(elmDataType);
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          final ILoaderFactory<WritableColumnVector> loader =
              SparkLoaderFactoryUtil.createLoaderFactory(vector);
        });
  }

  @Test
  void T_createLoaderFactory_MapType_3() {
    final int loadSize = 5;
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
    final DataType valueType = DataTypes.createStructType(fields);
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          final ILoaderFactory<WritableColumnVector> loader =
              SparkLoaderFactoryUtil.createLoaderFactory(vector);
        });
  }

  @Test
  void T_createLoaderFactory_MapType_4() {
    final int loadSize = 5;
    final DataType value2Type = DataTypes.BooleanType;
    final DataType valueType = DataTypes.createMapType(DataTypes.StringType, value2Type, true);
    final DataType dataType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          final ILoaderFactory<WritableColumnVector> loader =
              SparkLoaderFactoryUtil.createLoaderFactory(vector);
        });
  }
}
