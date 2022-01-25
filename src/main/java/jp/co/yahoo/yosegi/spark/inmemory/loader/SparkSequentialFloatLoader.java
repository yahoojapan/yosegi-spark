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

public class SparkSequentialFloatLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialFloatLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setValue(final int index, final PrimitiveObject value) throws IOException {
    setFloat(index, value.getFloat());
  }

  @Override
  public void setFloat(final int index, final float value) throws IOException {
    vector.putFloat(index, value);
  }
}
