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
package jp.co.yahoo.yosegi.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.yosegi.{ConvertYosegiHiveTables, PruneYosegiSchema}

/** Opt-in nested column pruning for Yosegi DataSource and Hive reads on Spark 3.5. */
class YosegiSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Reuse Hive's static partition pruning before choosing the read implementation.
    extensions.injectPreCBORule(spark => new ConvertYosegiHiveTables(spark))
    extensions.injectPreCBORule(spark => new PruneYosegiSchema(spark))
  }
}
