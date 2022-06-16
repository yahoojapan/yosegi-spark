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

public class SparkDictionaryDecimalLoader extends AbstractSparkDictionaryLoader {
  private final int precision;
  private final int scale;

  private Decimal[] dic;
  private boolean[] isNull;

  public SparkDictionaryDecimalLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    precision = ((DecimalType) vector.dataType()).precision();
    scale = ((DecimalType) vector.dataType()).scale();
  }

  @Override
  public void setBooleanToDic(final int index, final boolean value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setByteToDic(final int index, final byte value) throws IOException {
    setIntegerToDic(index, value);
  }

  @Override
  public void setShortToDic(final int index, final short value) throws IOException {
    setIntegerToDic(index, value);
  }

  @Override
  public void setIntegerToDic(final int index, final int value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    dic[index] = decimal;
  }

  @Override
  public void setLongToDic(final int index, final long value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    dic[index] = decimal;
  }

  @Override
  public void setFloatToDic(final int index, final float value) throws IOException {
    try {
      setDoubleToDic(index, new FloatObj(value).getDouble());
    } catch (final Exception e) {
      setNullToDic(index);
    }
  }

  @Override
  public void setDoubleToDic(final int index, final double value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    dic[index] = decimal;
  }

  //  @Override
  //  public void setBytesToDic(int index, byte[] value) throws IOException {
  //    IDictionaryLoader.super.setBytesToDic(index, value);
  //  }

  @Override
  public void setBytesToDic(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    try {
      setStringToDic(index, new BytesObj(value, start, length).getString());
    } catch (final Exception e) {
      setNullToDic(index);
    }
  }

  @Override
  public void setStringToDic(final int index, final String value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    dic[index] = decimal;
  }

  //  @Override
  //  public void setStringToDic(int index, char[] value) throws IOException {
  //    IDictionaryLoader.super.setStringToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setStringToDic(int index, char[] value, int start, int length) throws IOException
  // {
  //    IDictionaryLoader.super.setStringToDic(index, value, start, length);
  //  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dic = new Decimal[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(final int index, final int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putDecimal(index, dic[dicIndex], precision);
    }
  }

  @Override
  public void setNullToDic(final int index) throws IOException {
    isNull[index] = true;
  }
}
