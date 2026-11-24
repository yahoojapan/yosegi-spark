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
package jp.co.yahoo.yosegi.spark;

import java.util.NoSuchElementException;
import jp.co.yahoo.yosegi.spark.reader.IColumnarBatchReader;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InternalRowIteratorTest {

  @Test
  void rowModeSkipsEmptyBatchesAndPreservesReturnedRows() {
    final ReusingReader reader = new ReusingReader();
    final StructType schema = new StructType().add("value", DataTypes.IntegerType);
    final InternalRowIterator iterator = new InternalRowIterator(reader, false, schema);
    try {
      assertTrue(iterator.hasNext());
      final InternalRow first = (InternalRow) iterator.next();
      assertInstanceOf(UnsafeRow.class, first);
      assertEquals(1, first.getInt(0));

      assertTrue(iterator.hasNext());
      final InternalRow second = (InternalRow) iterator.next();
      assertEquals(2, second.getInt(0));
      assertEquals(1, first.getInt(0));

      assertFalse(iterator.hasNext());
      assertThrows(NoSuchElementException.class, iterator::next);
    } finally {
      iterator.close();
    }
    assertEquals(1, reader.closeCount);
  }

  @Test
  void originalConstructorStillReturnsBatches() {
    final ReusingReader reader = new ReusingReader();
    final InternalRowIterator iterator = new InternalRowIterator(reader);
    try {
      assertInstanceOf(ColumnarBatch.class, iterator.next());
    } finally {
      iterator.close();
    }
    assertEquals(1, reader.closeCount);
  }

  private static final class ReusingReader implements IColumnarBatchReader {
    private final OnHeapColumnVector column = new OnHeapColumnVector(1, DataTypes.IntegerType);
    private int index;
    private int closeCount;

    @Override
    public void setLineFilterNode(final IExpressionNode node) {}

    @Override
    public boolean hasNext() {
      return index < 3;
    }

    @Override
    public ColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final int rowCount = index++ == 0 ? 0 : 1;
      if (rowCount != 0) {
        column.reset();
        column.putInt(0, index - 1);
      }
      return new ColumnarBatch(new ColumnVector[] {column}, rowCount);
    }

    @Override
    public void close() {
      closeCount++;
      column.close();
    }
  }
}
