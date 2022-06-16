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

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DecimalType;

public class SparkUnionDecimalLoader extends AbstractSparkUnionLoader {
  private final int precision;
  private final int scale;

  public SparkUnionDecimalLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    precision = ((DecimalType) vector.dataType()).precision();
    scale = ((DecimalType) vector.dataType()).scale();
  }

  @Override
  protected void setValue(final int index, final WritableColumnVector childVector) {
    vector.putDecimal(index, childVector.getDecimal(index, precision, scale), precision);
  }

  @Override
  protected boolean isTargetColumnType(final ColumnType columnType) {
    switch (columnType) {
      case BYTE:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INTEGER:
      case LONG:
      case SHORT:
      case STRING:
        return true;
      default:
        return false;
    }
  }
}
