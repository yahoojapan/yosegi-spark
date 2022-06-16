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
package jp.co.yahoo.yosegi.spark.reader;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.reader.WrapReader;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SparkColumnarBatchReader implements IColumnarBatchReader {

  private final WrapReader<ColumnarBatch> reader;
  private final WritableColumnVector[] childColumns;

  public SparkColumnarBatchReader(
      final StructType partitionSchema,
      final InternalRow partitionValue,
      final StructType schema,
      final InputStream in,
      final long fileLength,
      final long start,
      final long length,
      final Configuration config,
      final IExpressionNode node)
      throws IOException {
    final StructField[] fields = schema.fields();
    childColumns = new OnHeapColumnVector[schema.length() + partitionSchema.length()];
    final Map<String, Integer> keyIndexMap = new HashMap<String, Integer>();
    for (int i = 0; i < fields.length; i++) {
      keyIndexMap.put(fields[i].name(), i);
      childColumns[i] = new OnHeapColumnVector(0, fields[i].dataType());
    }
    // NOTE: create reader
    final YosegiReader yosegiReader = new YosegiReader();
    yosegiReader.setNewStream(in, fileLength, config, start, length);
    yosegiReader.setBlockSkipIndex(node);
    final SparkColumnarBatchConverter converter =
        new SparkColumnarBatchConverter(
            schema, partitionSchema, partitionValue, keyIndexMap, childColumns);
    reader = new WrapReader<>(yosegiReader, converter);
  }

  @Override
  public void setLineFilterNode(final IExpressionNode node) {}

  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  @Override
  public ColumnarBatch next() throws IOException {
    if (!hasNext()) {
      final ColumnarBatch result = new ColumnarBatch(childColumns);
      result.setNumRows(0);
      return result;
    }
    return reader.next();
  }

  @Override
  public void close() throws Exception {
    reader.close();
    for (int i = 0; i < childColumns.length; i++) {
      childColumns[i].close();
    }
  }
}
