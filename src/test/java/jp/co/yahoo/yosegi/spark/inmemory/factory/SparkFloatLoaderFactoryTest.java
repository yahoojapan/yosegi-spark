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
package jp.co.yahoo.yosegi.spark.inmemory.factory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.maker.ConstantColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpUnionColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpFloatColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayFloatColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkConstFloatLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkDictionaryFloatLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkNullLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkSequentialFloatLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkUnionFloatLoader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkFloatLoaderFactoryTest {
  public static Stream<Arguments> D_floatColumnBinaryMaker() {
    return Stream.of(
        arguments(OptimizedNullArrayFloatColumnBinaryMaker.class.getName()),
        arguments(OptimizedNullArrayDumpFloatColumnBinaryMaker.class.getName()));
  }

  public static Stream<Arguments> D_constColumnBinaryMaker() {
    return Stream.of(arguments(ConstantColumnBinaryMaker.class.getName()));
  }

  public static Stream<Arguments> D_unionColumnBinaryMaker() {
    return Stream.of(arguments(DumpUnionColumnBinaryMaker.class.getName()));
  }

  @ParameterizedTest
  @MethodSource("D_floatColumnBinaryMaker")
  void T_createLoader_Sequential(final String binaryMakerClassName) throws IOException {
    final ColumnBinary columnBinary = Utils.dummyColumnBinary(binaryMakerClassName, false, false);

    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader = new SparkFloatLoaderFactory(vector).createLoader(columnBinary, loadSize);

    assertTrue(loader instanceof SparkSequentialFloatLoader);
  }

  @ParameterizedTest
  @MethodSource("D_floatColumnBinaryMaker")
  void T_createLoader_Dictionary(final String binaryMakerClassName) throws IOException {
    final ColumnBinary columnBinary = Utils.dummyColumnBinary(binaryMakerClassName, true, false);

    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader = new SparkFloatLoaderFactory(vector).createLoader(columnBinary, loadSize);

    // NOTE: float is always sequential.
    assertTrue(loader instanceof SparkSequentialFloatLoader);
  }

  @ParameterizedTest
  @MethodSource("D_constColumnBinaryMaker")
  void T_createLoader_Const(final String binaryMakerClassName) throws IOException {
    final ColumnBinary columnBinary = Utils.dummyColumnBinary(binaryMakerClassName, false, true);

    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader = new SparkFloatLoaderFactory(vector).createLoader(columnBinary, loadSize);

    assertTrue(loader instanceof SparkConstFloatLoader);
  }

  @ParameterizedTest
  @MethodSource("D_unionColumnBinaryMaker")
  void T_createLoader_Union(final String binaryMakerClassName) throws IOException {
    final ColumnBinary columnBinary = Utils.dummyColumnBinary(binaryMakerClassName, false, false);

    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader = new SparkFloatLoaderFactory(vector).createLoader(columnBinary, loadSize);

    assertTrue(loader instanceof SparkUnionFloatLoader);
  }

  @Test
  void T_createLoader_Null() throws IOException {
    final ColumnBinary columnBinary = null;

    final int loadSize = 5;
    final DataType dataType = DataTypes.FloatType;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader = new SparkFloatLoaderFactory(vector).createLoader(columnBinary, loadSize);

    assertTrue(loader instanceof SparkNullLoader);
  }
}
