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
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import org.apache.spark.sql.types.Decimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Utils {
  public static int getLoadSize(final int[] repetitions) {
    int loadSize = 0;
    for (final int repetition : repetitions) {
      loadSize += repetition;
    }
    return loadSize;
  }

  public static <M, P> IColumn toColumn(
      final Map<Integer, M> values,
      final int loadSize,
      final ColumnType columnType,
      final Function<M, P> function)
      throws IOException {
    final IColumn column = new PrimitiveColumn(columnType, "column");
    for (int i = 0; i < loadSize; i++) {
      if (values.containsKey(i)) {
        column.add(columnType, function.apply(values.get(i)), i);
      }
    }
    return column;
  }

  public static IColumn toBooleanColumn(final Map<Integer, Boolean> values, final int loadSize)
      throws IOException {
    final Function<Boolean, BooleanObj> function = s -> new BooleanObj(s);
    return Utils.<Boolean, BooleanObj>toColumn(values, loadSize, ColumnType.BOOLEAN, function);
  }

  public static IColumn toBytesColumn(final Map<Integer, String> values, final int loadSize)
      throws IOException {
    final Function<String, BytesObj> function =
        s -> new BytesObj(s.getBytes(StandardCharsets.UTF_8));
    return Utils.<String, BytesObj>toColumn(values, loadSize, ColumnType.BYTES, function);
  }

  public static IColumn toStringColumn(final Map<Integer, String> values, final int loadSize)
      throws IOException {
    final Function<String, StringObj> function = s -> new StringObj(s);
    return Utils.<String, StringObj>toColumn(values, loadSize, ColumnType.STRING, function);
  }

  public static IColumn toLongColumn(final Map<Integer, Long> values, final int loadSize)
      throws IOException {
    final Function<Long, LongObj> function = s -> new LongObj(s);
    return Utils.<Long, LongObj>toColumn(values, loadSize, ColumnType.LONG, function);
  }

  public static IColumn toIntegerColumn(final Map<Integer, Integer> values, final int loadSize)
      throws IOException {
    final Function<Integer, IntegerObj> function = s -> new IntegerObj(s);
    return Utils.<Integer, IntegerObj>toColumn(values, loadSize, ColumnType.INTEGER, function);
  }

  public static IColumn toShortColumn(final Map<Integer, Short> values, final int loadSize)
      throws IOException {
    final Function<Short, ShortObj> function = s -> new ShortObj(s);
    return Utils.<Short, ShortObj>toColumn(values, loadSize, ColumnType.SHORT, function);
  }

  public static IColumn toByteColumn(final Map<Integer, Byte> values, final int loadSize)
      throws IOException {
    final Function<Byte, ByteObj> function = s -> new ByteObj(s);
    return Utils.<Byte, ByteObj>toColumn(values, loadSize, ColumnType.BYTE, function);
  }

  public static IColumn toDoubleColumn(final Map<Integer, Double> values, final int loadSize)
      throws IOException {
    final Function<Double, DoubleObj> function = s -> new DoubleObj(s);
    return Utils.<Double, DoubleObj>toColumn(values, loadSize, ColumnType.DOUBLE, function);
  }

  public static IColumn toFloatColumn(final Map<Integer, Float> values, final int loadSize)
      throws IOException {
    final Function<Float, FloatObj> function = s -> new FloatObj(s);
    return Utils.<Float, FloatObj>toColumn(values, loadSize, ColumnType.FLOAT, function);
  }

  public static ColumnBinary getColumnBinary(
      final IColumnBinaryMaker binaryMaker,
      final IColumn column,
      ColumnBinaryMakerConfig config,
      ColumnBinaryMakerCustomConfigNode configNode,
      CompressResultNode resultNode)
      throws IOException {
    if (config == null) {
      config = new ColumnBinaryMakerConfig();
    }
    if (configNode == null) {
      configNode = new ColumnBinaryMakerCustomConfigNode("root", config);
    }
    if (resultNode == null) {
      resultNode = new CompressResultNode();
    }
    return binaryMaker.toBinary(config, configNode, resultNode, column);
  }

  public static ColumnType[] getColumnTypes() {
    return new ColumnType[] {
      ColumnType.UNION,
      ColumnType.ARRAY,
      ColumnType.SPREAD,
      ColumnType.BOOLEAN,
      ColumnType.BYTE,
      ColumnType.BYTES,
      ColumnType.DOUBLE,
      ColumnType.FLOAT,
      ColumnType.INTEGER,
      ColumnType.LONG,
      ColumnType.SHORT,
      ColumnType.STRING,
      ColumnType.NULL,
      ColumnType.EMPTY_ARRAY,
      ColumnType.EMPTY_SPREAD,
      ColumnType.MAP,
      ColumnType.STRUCT,
      ColumnType.UNKNOWN
    };
  }

  public static <T> Decimal valueToDecimal(final T value, final int precision, final int scale) {
    final Decimal v;
    if (value instanceof String) {
      v = Decimal.apply((String) value);
    } else if (value instanceof Double) {
      v = Decimal.apply((Double) value);
    } else if (value instanceof Float) {
      v = Decimal.apply((Float) value);
    } else if (value instanceof Byte) {
      v = Decimal.apply((Byte) value);
    } else if (value instanceof Short) {
      v = Decimal.apply((Short) value);
    } else if (value instanceof Integer) {
      v = Decimal.apply((Integer) value);
    } else {
      v = Decimal.apply((Long) value);
    }
    v.changePrecision(precision, scale);
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe((int) v.toUnscaledLong(), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(v.toUnscaledLong(), precision, scale);
    } else {
      return Decimal.apply(
          new BigDecimal(new BigInteger(v.toJavaBigDecimal().unscaledValue().toByteArray()), scale),
          precision,
          scale);
    }
  }

  public static ColumnBinary dummyColumnBinary(final String makerClassName, final Boolean isDictionary, Boolean isConst) {
    class DummyColumnBinary extends ColumnBinary {
      public DummyColumnBinary(
          final String makerClassName,
          final String compressorClassName,
          final String columnName,
          final ColumnType columnType,
          final int rowCount,
          final int rawDataSize,
          final int logicalDataSize,
          final int cardinality,
          final byte[] binary,
          final int binaryStart,
          final int binaryLength,
          final List<ColumnBinary> columnBinaryList) {
        super(
            makerClassName,
            compressorClassName,
            columnName,
            columnType,
            rowCount,
            rawDataSize,
            logicalDataSize,
            cardinality,
            binary,
            binaryStart,
            binaryLength,
            columnBinaryList);
      }
    }
    if (isDictionary) {
      final ColumnBinary columnBinary =
          new DummyColumnBinary(
              makerClassName, null, null, null, 0, 0, 0, 0, new byte[] {0}, 0, 0, null);
      columnBinary.isSetLoadSize = true;
      return columnBinary;
    } else if (isConst) {
      final ColumnBinary columnBinary =
          new DummyColumnBinary(
              makerClassName, null, null, null, 0, 0, 0, 0, new byte[] {0}, 0, 0, null);
      columnBinary.setRepetitions(new int[] {}, 0);
      return columnBinary;
    }
    final ColumnBinary columnBinary =
        new DummyColumnBinary(
            makerClassName, null, null, null, 0, 0, 0, 0, new byte[] {0}, 0, 0, null);
    columnBinary.isSetLoadSize = false;
    return columnBinary;
  }
}
