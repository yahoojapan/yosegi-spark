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
package jp.co.yahoo.yosegi.spark.utils;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProjectionPushdownUtilTest {

  @Test
  public void T_createProjectionPushdownJson_nullSchema() {
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(null);
    assertEquals("[]", json);
  }

  @Test
  public void T_createProjectionPushdownJson_emptySchema() {
    StructType schema = new StructType();
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals("[]", json);
  }

  @Test
  public void T_createProjectionPushdownJson_flatSchema() {
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("flag", DataTypes.BooleanType, true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals("[[\"id\"],[\"name\"],[\"flag\"]]", json);
  }

  @Test
  public void T_createProjectionPushdownJson_nestedStruct() {
    StructType structASchema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("leaf_1", DataTypes.LongType, true),
            DataTypes.createStructField("leaf_2", DataTypes.StringType, true)
        )
    );
    StructType structBSchema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("leaf_3", DataTypes.createArrayType(DataTypes.StringType), true),
            DataTypes.createStructField("leaf_4", DataTypes.StringType, true)
        )
    );
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("struct_a", structASchema, true),
            DataTypes.createStructField("struct_b", structBSchema, true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals(
        "[[\"struct_a\",\"leaf_1\"],[\"struct_a\",\"leaf_2\"],[\"struct_b\",\"leaf_3\"],[\"struct_b\",\"leaf_4\"]]",
        json
    );
  }

  @Test
  public void T_createProjectionPushdownJson_deeplyNestedStruct() {
    StructType level3 = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("c", DataTypes.IntegerType, true)
        )
    );
    StructType level2 = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("b", level3, true),
            DataTypes.createStructField("d", DataTypes.StringType, true)
        )
    );
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("a", level2, true),
            DataTypes.createStructField("top", DataTypes.DoubleType, true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals(
        "[[\"a\",\"b\",\"c\"],[\"a\",\"d\"],[\"top\"]]",
        json
    );
  }

  @Test
  public void T_createProjectionPushdownJson_structContainingArrayAndMap() {
    StructType innerStruct = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("f1", DataTypes.StringType, true)
        )
    );
    ArrayType arrayOfStruct = DataTypes.createArrayType(innerStruct);
    MapType mapOfStruct = DataTypes.createMapType(DataTypes.StringType, innerStruct);

    StructType structWithArrayAndMap = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("arr", arrayOfStruct, true),
            DataTypes.createStructField("map", mapOfStruct, true),
            DataTypes.createStructField("primitive", DataTypes.IntegerType, true)
        )
    );
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("s", structWithArrayAndMap, true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals(
        "[[\"s\",\"arr\"],[\"s\",\"map\"],[\"s\",\"primitive\"]]",
        json
    );
  }

  @Test
  public void T_createProjectionPushdownJson_topLevelArrayAndMap() {
    StructType inner = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("val", DataTypes.StringType, true)
        )
    );
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("arr", DataTypes.createArrayType(inner), true),
            DataTypes.createStructField("map", DataTypes.createMapType(DataTypes.StringType, inner), true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals("[[\"arr\"],[\"map\"]]", json);
  }

  @Test
  public void T_createProjectionPushdownJson_emptyNestedStruct() {
    StructType emptyStruct = new StructType();
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("empty_struct", emptyStruct, true),
            DataTypes.createStructField("id", DataTypes.IntegerType, true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals("[[\"empty_struct\"],[\"id\"]]", json);
  }

  @Test
  public void T_createProjectionPushdownJson_specialCharactersInFieldName() {
    StructType nested = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("col \"with\" quote", DataTypes.StringType, true)
        )
    );
    StructType schema = DataTypes.createStructType(
        Arrays.asList(
            DataTypes.createStructField("parent \"col\"", nested, true)
        )
    );
    String json = ProjectionPushdownUtil.createProjectionPushdownJson(schema);
    assertEquals("[[\"parent \\\"col\\\"\",\"col \\\"with\\\" quote\"]]", json);
  }

}
