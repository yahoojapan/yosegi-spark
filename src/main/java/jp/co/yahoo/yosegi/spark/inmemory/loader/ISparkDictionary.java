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

import org.apache.spark.sql.execution.vectorized.Dictionary;

import java.io.IOException;

public interface ISparkDictionary extends Dictionary {
  @Override
  default int decodeToInt(final int id) {
    throw new UnsupportedOperationException("decodeToInt is not supported.");
  }

  @Override
  default long decodeToLong(final int id) {
    throw new UnsupportedOperationException("decodeToLong is not supported.");
  }

  @Override
  default float decodeToFloat(final int id) {
    throw new UnsupportedOperationException("decodeToFloat is not supported.");
  }

  @Override
  default double decodeToDouble(final int id) {
    throw new UnsupportedOperationException("decodeToDouble is not supported.");
  }

  @Override
  default byte[] decodeToBinary(final int id) {
    throw new UnsupportedOperationException("decodeToBinary is not supported.");
  }

  default void setBytes(final int id, final byte[] value, final int start, final int length)
      throws IOException {
    throw new UnsupportedOperationException("setBytes is not supported.");
  }
}
