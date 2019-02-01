/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.message.parser.spark;

import java.io.IOException;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.catalyst.InternalRow;

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.spark.SparkSchemaFactory;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.IMessageReader;

public class SparkMessageReader implements IMessageReader {

  private final StructType schema;

  public SparkMessageReader( final IField schema ) throws IOException{
    this( (StructType)( SparkSchemaFactory.getDataType( schema ) ) );
  }

  public SparkMessageReader( final StructType schema ) throws IOException{
    this.schema = schema;
  }

  public IParser create( final InternalRow record ) throws IOException{
    return new SparkStructParser( schema , record );
  }

  @Override
  public IParser create( final byte[] message ) throws IOException{
    throw new UnsupportedOperationException( "Unsupport create( final byte[] message )" );
  }

  @Override
  public IParser create( final byte[] message , final int start , final int length ) throws IOException{
    throw new UnsupportedOperationException( "Unsupport create( final byte[] message , final int start , final int length )" );
  }

}
