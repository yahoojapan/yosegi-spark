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

public abstract class AbstractSparkSequentialNumberLoader extends AbstractSparkSequentialLoader {

  public AbstractSparkSequentialNumberLoader(
      final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  public abstract void setValue(int index, PrimitiveObject value) throws IOException;

  public void setPrimitiveObject(final int index, final PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setValue(index, value);
      } catch (final Exception e) {
        setNull(index);
      }
    }
  }

  // @Override
  // public void setBoolean(int index, boolean value) throws IOException {}

  @Override
  public void setByte(final int index, final byte value) throws IOException {
    setPrimitiveObject(index, new ByteObj(value));
  }

  @Override
  public void setShort(final int index, final short value) throws IOException {
    setPrimitiveObject(index, new ShortObj(value));
  }

  @Override
  public void setInteger(final int index, final int value) throws IOException {
    setPrimitiveObject(index, new IntegerObj(value));
  }

  @Override
  public void setLong(final int index, final long value) throws IOException {
    setPrimitiveObject(index, new LongObj(value));
  }

  @Override
  public void setFloat(final int index, final float value) throws IOException {
    setPrimitiveObject(index, new FloatObj(value));
  }

  @Override
  public void setDouble(final int index, final double value) throws IOException {
    setPrimitiveObject(index, new DoubleObj(value));
  }

  // @Override
  // public void setBytes(int index, byte[] value) throws IOException {}

  @Override
  public void setBytes(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    setPrimitiveObject(index, new BytesObj(value, start, length));
  }

  @Override
  public void setString(final int index, final String value) throws IOException {
    setPrimitiveObject(index, new StringObj(value));
  }

  // @Override
  // public void setString(int index, char[] value) throws IOException {}

  // @Override
  // public void setString(int index, char[] value, int start, int length) throws IOException {}
}
