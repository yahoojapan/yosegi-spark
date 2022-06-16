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

public abstract class AbstractSparkConstNumberLoader extends AbstractSparkConstLoader {
  protected WritableColumnVector idxVector;
  protected ISparkDictionary dic;

  public AbstractSparkConstNumberLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  public abstract void setConstFromValue(PrimitiveObject value) throws IOException;

  public void setConstFromPrimitiveObject(final PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromValue(value);
      } catch (final Exception e) {
        setConstFromNull();
      }
    }
  }

  // @Override
  // public void setConstFromBoolean(boolean value) throws IOException {}

  @Override
  public void setConstFromByte(final byte value) throws IOException {
    setConstFromPrimitiveObject(new ByteObj(value));
  }

  @Override
  public void setConstFromShort(final short value) throws IOException {
    setConstFromPrimitiveObject(new ShortObj(value));
  }

  @Override
  public void setConstFromInteger(final int value) throws IOException {
    setConstFromPrimitiveObject(new IntegerObj(value));
  }

  @Override
  public void setConstFromLong(final long value) throws IOException {
    setConstFromPrimitiveObject(new LongObj(value));
  }

  @Override
  public void setConstFromFloat(final float value) throws IOException {
    setConstFromPrimitiveObject(new FloatObj(value));
  }

  @Override
  public void setConstFromDouble(final double value) throws IOException {
    setConstFromPrimitiveObject(new DoubleObj(value));
  }

  // @Override
  // public void setConstFromBytes(byte[] value) throws IOException {}

  @Override
  public void setConstFromBytes(final byte[] value, final int start, final int length)
      throws IOException {
    setConstFromPrimitiveObject(new BytesObj(value, start, length));
  }

  @Override
  public void setConstFromString(final String value) throws IOException {
    setConstFromPrimitiveObject(new StringObj(value));
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
