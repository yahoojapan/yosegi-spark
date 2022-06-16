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
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryBooleanLoader extends AbstractSparkDictionaryLoader {
  private boolean[] dic;
  private boolean[] isNull;

  public SparkDictionaryBooleanLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  //  @Override
  //  public LoadType getLoaderType() {
  //    return IDictionaryLoader.super.getLoaderType();
  //  }

  @Override
  public void setBooleanToDic(final int index, final boolean value) throws IOException {
    dic[index] = value;
  }

  @Override
  public void setByteToDic(final int index, final byte value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setShortToDic(final int index, final short value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setIntegerToDic(final int index, final int value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setLongToDic(final int index, final long value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setFloatToDic(final int index, final float value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setDoubleToDic(final int index, final double value) throws IOException {
    setNullToDic(index);
  }

  //  @Override
  //  public void setBytesToDic(int index, byte[] value) throws IOException {
  //    IDictionaryLoader.super.setBytesToDic(index, value);
  //  }

  @Override
  public void setBytesToDic(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    try {
      setBooleanToDic(index, new BytesObj(value, start, length).getBoolean());
    } catch (final Exception e) {
      setNullToDic(index);
    }
  }

  @Override
  public void setStringToDic(final int index, final String value) throws IOException {
    try {
      setBooleanToDic(index, new StringObj(value).getBoolean());
    } catch (final Exception e) {
      setNullToDic(index);
    }
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
    dic = new boolean[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(final int index, final int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putBoolean(index, dic[dicIndex]);
    }
  }

  @Override
  public void setNullToDic(final int index) throws IOException {
    // FIXME: this method is not used in yosegi
    isNull[index] = true;
  }
}
