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

import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import java.io.IOException;

public class SparkSequentialDecimalLoader extends AbstractSparkSequentialLoader {
  private final int precision;
  private final int scale;

  public SparkSequentialDecimalLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    precision = ((DecimalType) vector.dataType()).precision();
    scale = ((DecimalType) vector.dataType()).scale();
  }

  //  @Override
  //  public void setBoolean(int index, boolean value) throws IOException {
  //    ISequentialLoader.super.setBoolean(index, value);
  //  }

  @Override
  public void setByte(final int index, final byte value) throws IOException {
    setInteger(index, value);
  }

  @Override
  public void setShort(final int index, final short value) throws IOException {
    setInteger(index, value);
  }

  @Override
  public void setInteger(final int index, final int value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    vector.putDecimal(index, decimal, precision);
  }

  @Override
  public void setLong(final int index, final long value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    vector.putDecimal(index, decimal, precision);
  }

  @Override
  public void setFloat(final int index, final float value) throws IOException {
    try {
      setDouble(index, new FloatObj(value).getDouble());
    } catch (final Exception e) {
      setNull(index);
    }
  }

  @Override
  public void setDouble(final int index, final double value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    vector.putDecimal(index, decimal, precision);
  }

  //  @Override
  //  public void setBytes(int index, byte[] value) throws IOException {
  //    ISequentialLoader.super.setBytes(index, value);
  //  }

  @Override
  public void setBytes(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    try {
      setString(index, new BytesObj(value, start, length).getString());
    } catch (final Exception e) {
      setNull(index);
    }
  }

  @Override
  public void setString(final int index, final String value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    vector.putDecimal(index, decimal, precision);
  }

  //  @Override
  //  public void setString(int index, char[] value) throws IOException {
  //    ISequentialLoader.super.setString(index, value);
  //  }
  //
  //  @Override
  //  public void setString(int index, char[] value, int start, int length) throws IOException {
  //    ISequentialLoader.super.setString(index, value, start, length);
  //  }
}
