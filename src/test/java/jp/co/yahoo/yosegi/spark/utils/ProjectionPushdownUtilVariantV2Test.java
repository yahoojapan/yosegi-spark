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
package jp.co.yahoo.yosegi.spark.utils;

import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProjectionPushdownUtilVariantV2Test {
  @Test
  void T_createProjectionPushdownJson_VariantExtractionStruct() {
    final StructType variantStruct =
        new StructType(
            new StructField[] {
              new StructField(
                  "0", DataTypes.LongType, true, createVariantMetadata("$.id")),
              new StructField(
                  "1", DataTypes.StringType, true, createVariantMetadata("$.name"))
            });
    final StructType schema =
        new StructType()
            .add("row_id", DataTypes.LongType)
            .add("v", variantStruct);
    assertEquals(
        "[[\"row_id\"],[\"v\",\"id\"],[\"v\",\"name\"]]",
        ProjectionPushdownUtil.createProjectionPushdownJson(schema));
  }

  @Test
  void T_createProjectionPushdownJson_NestedVariantExtractionStruct() {
    final StructType variantStruct =
        new StructType(
            new StructField[] {
              new StructField(
                  "0", DataTypes.LongType, true, createVariantMetadata("$.user.id")),
              new StructField(
                  "1", DataTypes.StringType, true,
                  createVariantMetadata("$.user.profile.name"))
            });
    final StructType schema = new StructType().add("v", variantStruct);
    assertEquals(
        "[[\"v\",\"user\",\"id\"],[\"v\",\"user\",\"profile\",\"name\"]]",
        ProjectionPushdownUtil.createProjectionPushdownJson(schema));
  }


  @Test
  void T_createProjectionPushdownJson_ArrayVariantExtractionReadsWholeArrayColumn() {
    final StructType variantStruct =
        new StructType(
            new StructField[] {
              new StructField(
                  "0", DataTypes.IntegerType, true, createVariantMetadata("$.items[0]")),
              new StructField(
                  "1", DataTypes.IntegerType, true, createVariantMetadata("$.items[1]")),
              new StructField(
                  "2", DataTypes.LongType, true, createVariantMetadata("$.users[0].id"))
            });
    final StructType schema = new StructType().add("v", variantStruct);
    assertEquals(
        "[[\"v\",\"items\"],[\"v\",\"users\"]]",
        ProjectionPushdownUtil.createProjectionPushdownJson(schema));
  }

  @Test
  void T_createProjectionPushdownJson_NormalStructPushesDownNestedFields() {
    final StructType normalStruct =
        new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType);
    final StructType schema = new StructType().add("v", normalStruct);
    assertEquals(
        "[[\"v\",\"id\"],[\"v\",\"name\"]]", ProjectionPushdownUtil.createProjectionPushdownJson(schema));
  }

  private static Metadata createVariantMetadata(final String path) {
    final Metadata variantMetadata =
        new MetadataBuilder()
            .putString(VariantStructUtil.PATH_METADATA_KEY, path)
            .putBoolean(VariantStructUtil.FAIL_ON_ERROR_METADATA_KEY, true)
            .putString(VariantStructUtil.TIME_ZONE_ID_METADATA_KEY, "UTC")
            .build();
    return new MetadataBuilder()
        .putMetadata(VariantStructUtil.VARIANT_METADATA_KEY, variantMetadata)
        .build();
  }
}
