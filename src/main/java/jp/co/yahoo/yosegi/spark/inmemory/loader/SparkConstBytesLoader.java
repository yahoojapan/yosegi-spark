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

public class SparkConstBytesLoader extends AbstractSparkConstLoader {

  private class SparkBytesConstDictionary implements ISparkDictionary {

    private byte[] binary;
    private int start;
    private int length;

    public SparkBytesConstDictionary() {}

    @Override
    public byte[] decodeToBinary(final int id) {
      final byte[] result = new byte[length];
      System.arraycopy(binary, start, result, 0, result.length);
      return result;
    }

    @Override
    public void setBytes(final int id, final byte[] value, final int start, final int length)
        throws IOException {
      binary = value;
      this.start = start;
      this.length = length;
    }
  }

  private final WritableColumnVector idxVector;
  private final ISparkDictionary dic;

  public SparkConstBytesLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    idxVector = this.vector.reserveDictionaryIds(loadSize);
    dic = new SparkBytesConstDictionary();
    this.vector.setDictionary(dic);
  }

  @Override
  public void setConstFromBoolean(final boolean value) throws IOException {
    setConstFromString(new BooleanObj(value).getString());
  }

  @Override
  public void setConstFromByte(final byte value) throws IOException {
    setConstFromString(new ByteObj(value).getString());
  }

  @Override
  public void setConstFromShort(final short value) throws IOException {
    setConstFromString(new ShortObj(value).getString());
  }

  @Override
  public void setConstFromInteger(final int value) throws IOException {
    setConstFromString(new IntegerObj(value).getString());
  }

  @Override
  public void setConstFromLong(final long value) throws IOException {
    setConstFromString(new LongObj(value).getString());
  }

  @Override
  public void setConstFromFloat(final float value) throws IOException {
    setConstFromString(new FloatObj(value).getString());
  }

  @Override
  public void setConstFromDouble(final double value) throws IOException {
    setConstFromString(new DoubleObj(value).getString());
  }

  // @Override
  // public void setConstFromBytes(byte[] value) throws IOException {}

  @Override
  public void setConstFromBytes(final byte[] value, final int start, final int length)
      throws IOException {
    // FIXME: how to use setIsConstant
    dic.setBytes(0, value, start, length);
    idxVector.putInts(0, loadSize, 0);
  }

  @Override
  public void setConstFromString(final String value) throws IOException {
    setConstFromBytes(new StringObj(value).getBytes());
  }

  // @Override
  // public void setConstFromString(char[] value) throws IOException {}

  // @Override
  // public void setConstFromString(char[] value, int start, int length) throws IOException {}

  @Override
  public void setConstFromNull() throws IOException {
    // FIXME:
    vector.putNulls(0, loadSize);
  }
}
