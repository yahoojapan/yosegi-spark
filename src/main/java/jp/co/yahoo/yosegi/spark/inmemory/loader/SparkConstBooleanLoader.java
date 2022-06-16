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

public class SparkConstBooleanLoader extends AbstractSparkConstLoader {
  public SparkConstBooleanLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  //  @Override
  //  public LoadType getLoaderType() {
  //    return IConstLoader.super.getLoaderType();
  //  }

  @Override
  public void setConstFromBoolean(final boolean value) throws IOException {
    vector.putBooleans(0, loadSize, value);
  }

  //  @Override
  //  public void setConstFromByte(byte value) throws IOException {
  //    IConstLoader.super.setConstFromByte(value);
  //  }
  //
  //  @Override
  //  public void setConstFromShort(short value) throws IOException {
  //    IConstLoader.super.setConstFromShort(value);
  //  }
  //
  //  @Override
  //  public void setConstFromInteger(int value) throws IOException {
  //    IConstLoader.super.setConstFromInteger(value);
  //  }
  //
  //  @Override
  //  public void setConstFromLong(long value) throws IOException {
  //    IConstLoader.super.setConstFromLong(value);
  //  }
  //
  //  @Override
  //  public void setConstFromFloat(float value) throws IOException {
  //    IConstLoader.super.setConstFromFloat(value);
  //  }
  //
  //  @Override
  //  public void setConstFromDouble(double value) throws IOException {
  //    IConstLoader.super.setConstFromDouble(value);
  //  }
  //
  //  @Override
  //  public void setConstFromBytes(byte[] value) throws IOException {
  //    IConstLoader.super.setConstFromBytes(value);
  //  }

  @Override
  public void setConstFromBytes(final byte[] value, final int start, final int length)
      throws IOException {
    try {
      setConstFromBoolean(new BytesObj(value, start, length).getBoolean());
    } catch (final Exception e) {
      setConstFromNull();
    }
  }

  @Override
  public void setConstFromString(final String value) throws IOException {
    try {
      setConstFromBoolean(new StringObj(value).getBoolean());
    } catch (final Exception e) {
      setConstFromNull();
    }
  }

  //  @Override
  //  public void setConstFromString(char[] value) throws IOException {
  //    IConstLoader.super.setConstFromString(value);
  //  }
  //
  //  @Override
  //  public void setConstFromString(char[] value, int start, int length) throws IOException {
  //    IConstLoader.super.setConstFromString(value, start, length);
  //  }

  @Override
  public void setConstFromNull() throws IOException {
    vector.putNulls(0, loadSize);
  }
}
