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

import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public abstract class AbstractSparkDictionaryNumberLoader extends AbstractSparkDictionaryLoader {
  protected WritableColumnVector idxVector;
  protected ISparkDictionary dic;
  protected boolean[] isNull;

  public AbstractSparkDictionaryNumberLoader(
      final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  public abstract void setValueToDic(int index, PrimitiveObject value) throws IOException;

  public void setPrimitiveObjectToDic(final int index, final PrimitiveObject value)
      throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setValueToDic(index, value);
      } catch (final Exception e) {
        setNullToDic(index);
      }
    }
  }

  // @Override
  // public void createDictionary(int dictionarySize) throws IOException {}

  @Override
  public void setDictionaryIndex(final int index, final int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      idxVector.putInt(index, dicIndex);
    }
  }

  // @Override
  // public void setBooleanToDic(int index, boolean value) throws IOException {}

  @Override
  public void setByteToDic(final int index, final byte value) throws IOException {
    setPrimitiveObjectToDic(index, new ByteObj(value));
  }

  @Override
  public void setShortToDic(final int index, final short value) throws IOException {
    setPrimitiveObjectToDic(index, new ShortObj(value));
  }

  @Override
  public void setIntegerToDic(final int index, final int value) throws IOException {
    setPrimitiveObjectToDic(index, new IntegerObj(value));
  }

  @Override
  public void setLongToDic(final int index, final long value) throws IOException {
    setPrimitiveObjectToDic(index, new LongObj(value));
  }

  @Override
  public void setFloatToDic(final int index, final float value) throws IOException {
    setPrimitiveObjectToDic(index, new FloatObj(value));
  }

  @Override
  public void setDoubleToDic(final int index, final double value) throws IOException {
    setPrimitiveObjectToDic(index, new DoubleObj(value));
  }

  // @Override
  // public void setBytesToDic(int index, byte[] value) throws IOException {}

  @Override
  public void setBytesToDic(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    setPrimitiveObjectToDic(index, new BytesObj(value, start, length));
  }

  @Override
  public void setStringToDic(final int index, final String value) throws IOException {
    setPrimitiveObjectToDic(index, new StringObj(value));
  }

  // @Override
  // public void setStringToDic(int index, char[] value) throws IOException {}

  // @Override
  // public void setStringToDic(int index, char[] value, int start, int length) throws IOException
  // {}

  @Override
  public void setNullToDic(final int index) throws IOException {
    // FIXME: this method is not used in yosegi
    isNull[index] = true;
  }
}
