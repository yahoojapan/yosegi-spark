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

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryBytesLoader extends AbstractSparkDictionaryLoader {

  private class SparkBytesDictionary implements ISparkDictionary {

    private final byte[][] binaryLinkArray;
    private final int[] startArray;
    private final int[] lengthArray;

    public SparkBytesDictionary(final int dicSize) {
      binaryLinkArray = new byte[dicSize][];
      startArray = new int[dicSize];
      lengthArray = new int[dicSize];
    }

    @Override
    public byte[] decodeToBinary(final int id) {
      final byte[] result = new byte[lengthArray[id]];
      System.arraycopy(binaryLinkArray[id], startArray[id], result, 0, result.length);
      return result;
    }

    @Override
    public void setBytes(final int id, final byte[] value, final int start, final int length)
        throws IOException {
      binaryLinkArray[id] = value;
      startArray[id] = start;
      lengthArray[id] = length;
    }
  }

  private WritableColumnVector idxVector;
  private ISparkDictionary dic;

  public SparkDictionaryBytesLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    idxVector = vector.reserveDictionaryIds(loadSize);
    dic = new SparkBytesDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setDictionaryIndex(final int index, final int dicIndex) throws IOException {
    idxVector.putInt(index, dicIndex);
  }

  @Override
  public void setBooleanToDic(final int index, final boolean value) throws IOException {
    setStringToDic(index, new BooleanObj(value).getString());
  }

  @Override
  public void setByteToDic(final int index, final byte value) throws IOException {
    setStringToDic(index, new ByteObj(value).getString());
  }

  @Override
  public void setShortToDic(final int index, final short value) throws IOException {
    setStringToDic(index, new ShortObj(value).getString());
  }

  @Override
  public void setIntegerToDic(final int index, final int value) throws IOException {
    setStringToDic(index, new IntegerObj(value).getString());
  }

  @Override
  public void setLongToDic(final int index, final long value) throws IOException {
    setStringToDic(index, new LongObj(value).getString());
  }

  @Override
  public void setFloatToDic(final int index, final float value) throws IOException {
    setStringToDic(index, new FloatObj(value).getString());
  }

  @Override
  public void setDoubleToDic(final int index, final double value) throws IOException {
    setStringToDic(index, new DoubleObj(value).getString());
  }

  // @Override
  // public void setBytesToDic(int index, byte[] value) throws IOException {}

  @Override
  public void setBytesToDic(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    dic.setBytes(index, value, start, length);
  }

  @Override
  public void setStringToDic(final int index, final String value) throws IOException {
    setBytesToDic(index, new StringObj(value).getBytes());
  }

  // @Override
  // public void setStringToDic(int index, char[] value) throws IOException {}

  // @Override
  // public void setStringToDic(int index, char[] value, int start, int length) throws IOException
  // {}

  @Override
  public void setNullToDic(final int index) throws IOException {
    // FIXME: this method is not used in yosegi
  }
}
