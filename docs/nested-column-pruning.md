<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Nested column pruning in Spark 3.5

Spark 3.5's built-in physical schema pruning handles Parquet and ORC. This extension applies schema pruning to Yosegi DataSource reads and supported Hive tables, so the reader can skip unused nested fields.

## Configuration

Set the extension before creating the SparkSession:

```scala
val spark = SparkSession.builder()
  .config("spark.sql.extensions", "jp.co.yahoo.yosegi.spark.YosegiSparkSessionExtensions")
  .enableHiveSupport()
  .getOrCreate()
```

When other extensions are configured, append this class to the comma-separated list rather than replacing existing extensions. For DataSource-only use, enableHiveSupport is unnecessary; the extension requires Spark's spark-hive module on the classpath to support Hive table conversion. Use the artifact built for Spark 3.5 and Scala 2.12.

Both `spark.sql.yosegi.enabled` and `spark.sql.optimizer.nestedSchemaPruning.enabled` default to `true`. The former is specific to this extension; without the extension, it has no effect.

```sql
-- Disable nested pruning while keeping supported Hive reads on the Yosegi reader.
SET spark.sql.optimizer.nestedSchemaPruning.enabled=false;

-- Disable this extension's read conversion and pruning for new query plans.
SET spark.sql.yosegi.enabled=false;
```

These settings affect newly planned queries, not cached DataFrames or running jobs.

## Example

Assume a DataSource table with `payload STRUCT<id: BIGINT, description: STRING>` and `items ARRAY<STRUCT<code: STRING, details: STRING>>`:

```sql
SELECT payload.id FROM events;

SELECT item.code
FROM events
LATERAL VIEW explode(items) expanded AS item;
```

Use `EXPLAIN` to inspect `ReadSchema`. With nested pruning enabled, the first query reads `payload.id` without `payload.description`; the second can read `items.code` without `items.details`. Fields used only by filters remain in the read schema. Whole-Struct or whole-element references retain the corresponding full value.

## Hive tables

Existing SQL can use the same optimization without changing the table definition. The extension rewrites eligible Hive reads into Yosegi file scans and retains metastore partition locations and partition values. Static Hive partition pruning runs before conversion.

Eligible tables are external, unbucketed Hive tables with the standard `jp.co.yahoo.yosegi.hive.YosegiSerde` and `jp.co.yahoo.yosegi.hive.io.YosegiHiveLineInputFormat`. They must use the ordinary storage properties supported by the extension. Custom SerDe properties, transactional/storage-handler settings, unsupported requested types, and differing partition storage settings keep the original Hive path. The extension does not convert INSERT or DDL commands.

Expand/flatten transformations keep their existing Hive read behavior. Requested data fields of unsupported reader types, including DATE fields and maps with complex values or non-string keys, also keep the Hive path. DATE partition columns are supported because Spark supplies partition values separately.

## Collection and reader behavior

Array element Struct fields can be pruned, including nested arrays. Maps remain whole. Expand/flatten and optional Arrow reader settings do not enable the new nested schema pruning rule.

The extension follows Spark 3.5's existing optimizer behavior for `explode`: some single-field projections can be pushed through Generate, while intervening filters or multiple element-field references can leave a full Struct or array. The extension does not introduce a separate Generate rewrite or promise pruning for every query shape.

The standard file reader honors Spark's `returning_batch` setting. Queries that use row-mode scans, including wide schemas or whole-stage code generation disabled, receive rows rather than columnar batches.

## Verification

Tests use synthetic data and verify returned values, exact required schemas, and input-byte reduction. The I/O fixture consumes selected values rather than using a count-only scan. Performance depends on data distribution and the fraction of required fields; no fixed speedup is guaranteed.
