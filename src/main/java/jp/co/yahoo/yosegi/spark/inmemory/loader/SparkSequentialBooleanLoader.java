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

public class SparkSequentialBooleanLoader extends AbstractSparkSequentialLoader {
  public SparkSequentialBooleanLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  //  @Override
  //  public LoadType getLoaderType() {
  //    return ISequentialLoader.super.getLoaderType();
  //  }

  @Override
  public void setBoolean(final int index, final boolean value) throws IOException {
    vector.putBoolean(index, value);
  }

  //  @Override
  //  public void setByte(int index, byte value) throws IOException {
  //    ISequentialLoader.super.setByte(index, value);
  //  }
  //
  //  @Override
  //  public void setShort(int index, short value) throws IOException {
  //    ISequentialLoader.super.setShort(index, value);
  //  }
  //
  //  @Override
  //  public void setInteger(int index, int value) throws IOException {
  //    ISequentialLoader.super.setInteger(index, value);
  //  }
  //
  //  @Override
  //  public void setLong(int index, long value) throws IOException {
  //    ISequentialLoader.super.setLong(index, value);
  //  }
  //
  //  @Override
  //  public void setFloat(int index, float value) throws IOException {
  //    ISequentialLoader.super.setFloat(index, value);
  //  }
  //
  //  @Override
  //  public void setDouble(int index, double value) throws IOException {
  //    ISequentialLoader.super.setDouble(index, value);
  //  }
  //
  //  @Override
  //  public void setBytes(int index, byte[] value) throws IOException {
  //    ISequentialLoader.super.setBytes(index, value);
  //  }

  @Override
  public void setBytes(final int index, final byte[] value, final int start, final int length)
      throws IOException {
    try {
      setBoolean(index, new BytesObj(value, start, length).getBoolean());
    } catch (final Exception e) {
      setNull(index);
    }
  }

  @Override
  public void setString(final int index, final String value) throws IOException {
    try {
      setBoolean(index, new StringObj(value).getBoolean());
    } catch (final Exception e) {
      setNull(index);
    }
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
