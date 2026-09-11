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

import com.fasterxml.jackson.databind.ObjectMapper;
import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public final class ProjectionPushdownUtil {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ProjectionPushdownUtil() {}

  public static String createProjectionPushdownJson(final StructType requiredSchema) {
    if (requiredSchema == null || requiredSchema.fields().length == 0) {
      return "[]";
    }
    final List<List<String>> paths = new ArrayList<>();
    collectPaths(requiredSchema, new ArrayList<String>(), paths);
    try {
      return OBJECT_MAPPER.writeValueAsString(paths);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void collectPaths(
      final StructType structType,
      final List<String> currentPath,
      final List<List<String>> result) {
    for (StructField field : structType.fields()) {
      final List<String> nextPath = new ArrayList<>(currentPath);
      nextPath.add(field.name());
      final DataType dataType = field.dataType();
      if (dataType instanceof StructType) {
        final StructType childStruct = (StructType) dataType;
        if (VariantStructUtil.isVariantStruct(childStruct)) {
          appendVariantProjections(result, nextPath, field);
        } else if (childStruct.fields().length > 0) {
          collectPaths(childStruct, nextPath, result);
        } else {
          result.add(nextPath);
        }
      } else {
        result.add(nextPath);
      }
    }
  }

  private static void appendVariantProjections(
      final List<List<String>> paths,
      final List<String> currentPath,
      final StructField variantField) {
    final StructType variantStruct = (StructType) variantField.dataType();
    for (StructField extractedField : variantStruct.fields()) {
      final String[] sourcePath =
          VariantStructUtil.getPhysicalProjectionPath(extractedField.metadata());
      final List<String> projectionPath = new ArrayList<>(currentPath);
      if (sourcePath != null) {
        for (String s : sourcePath) {
          projectionPath.add(s);
        }
      }
      if (!paths.contains(projectionPath)) {
        paths.add(projectionPath);
      }
    }
  }
}
