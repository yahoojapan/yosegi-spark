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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryDoubleLoader extends AbstractSparkDictionaryNumberLoader {
  private class SparkDoubleDictionary implements ISparkDictionary {
    private final double[] dic;

    public SparkDoubleDictionary(final int dicSize) {
      this.dic = new double[dicSize];
    }

    @Override
    public int decodeToInt(final int id) {
      return (int) dic[id];
    }

    @Override
    public long decodeToLong(final int id) {
      return (long) dic[id];
    }

    @Override
    public float decodeToFloat(final int id) {
      return (float) dic[id];
    }

    @Override
    public double decodeToDouble(final int id) {
      return dic[id];
    }

    public void set(final int index, final double value) {
      dic[index] = value;
    }
  }

  public SparkDictionaryDoubleLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setValueToDic(final int index, final PrimitiveObject value) throws IOException {
    setDoubleToDic(index, value.getDouble());
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    idxVector = vector.reserveDictionaryIds(loadSize);
    dic = new SparkDoubleDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setDoubleToDic(final int index, final double value) throws IOException {
    ((SparkDoubleDictionary) dic).set(index, value);
  }
}
