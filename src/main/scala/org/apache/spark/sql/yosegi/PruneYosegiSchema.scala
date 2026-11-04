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
package org.apache.spark.sql.yosegi

import jp.co.yahoo.yosegi.spark.YosegiFileFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SchemaPruning.{identifyRootFields, pruneSchema}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils.restoreOriginalOutputNames

/** Spark 3.5's built-in physical schema pruning only handles Parquet and ORC. */
class PruneYosegiSchema(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!spark.conf.get("spark.sql.yosegi.enabled", "true").toBoolean ||
        !spark.sessionState.conf.nestedSchemaPruningEnabled) return plan
    plan.transformDown {
      case op @ ScanOperation(projects, stayUp, pushDown,
          relation @ LogicalRelation(fs: HadoopFsRelation, _, _, _))
          if fs.fileFormat.isInstanceOf[YosegiFileFormat] && !hasReaderTransforms(fs.options) =>
        val names = relation.output.map(a => a.exprId -> a.name).toMap
        def normalize(e: Expression): Expression = e.transform {
          case a: AttributeReference if names.contains(a.exprId) => a.withName(names(a.exprId))
        }
        val normalizedProjects = projects.map(p => normalize(p).asInstanceOf[NamedExpression])
        val filters = pushDown.reduceOption(And).toSeq ++ stayUp
        val normalizedFilters = filters.map(normalize)
        val roots = identifyRootFields(normalizedProjects, normalizedFilters)
        if (roots.isEmpty || !roots.exists(r => !r.derivedFromAtt)) op
        else {
          val requested = pruneSchema(fs.dataSchema, roots)
          val pruned = YosegiReadSchema.keepMapsWhole(requested, fs.dataSchema)
            .asInstanceOf[StructType]
          if (pruned == fs.dataSchema) op
          else {
            val newFs = fs.copy(dataSchema = pruned)(spark)
            val fields = newFs.schema.fields.map(f => f.name -> f.dataType).toMap
            val output = relation.output.map(a => a.withDataType(fields.getOrElse(a.name, a.dataType)))
            val newRelation = relation.copy(relation = newFs, output = output)
            newRelation.copyTagsFrom(relation)
            val projection = ProjectionOverSchema(newFs.schema, AttributeSet(relation.output))
            def rewrite(e: Expression): Expression = e.transformDown {
              case projection(replacement) => replacement
            }
            val child = normalizedFilters.foldLeft[LogicalPlan](newRelation) { (p, f) =>
              Filter(rewrite(f), p)
            }
            val newProjects = normalizedProjects.map(p => rewrite(p).asInstanceOf[NamedExpression])
            Project(restoreOriginalOutputNames(newProjects, projects.map(_.name)), child)
          }
        }
    }
  }

  private def hasReaderTransforms(options: Map[String, String]): Boolean =
    options.get("spark.yosegi.enable.arrow.reader").exists(_.equalsIgnoreCase("true")) ||
      options.keys.exists(key =>
        key.startsWith("spread.reader.expand.") || key.startsWith("spread.reader.flatten."))
}
