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
package jp.co.yahoo.yosegi.spark.pushdown;

import java.util.Objects;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.spark.sql.sources.*;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;


public final class FilterConnectorFactory {
  private static final Map<Class, OperatorFilterFactory> gtDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> geDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> ltDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> leDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> eqDispatch = new HashMap<>();
  private static final Map<Class, ExpressionNodeFactory> dispatch   = new HashMap<>();

  static {
    gtDispatch.put(String.class, (v) -> new GtStringCompareFilter(v.toString()));
    geDispatch.put(String.class, (v) -> new GeStringCompareFilter(v.toString()));
    ltDispatch.put(String.class, (v) -> new LtStringCompareFilter(v.toString()));
    leDispatch.put(String.class, (v) -> new LeStringCompareFilter(v.toString()));
    eqDispatch.put(String.class, (v) -> new PerfectMatchStringFilter(v.toString()));

    setDispatchMap(gtDispatch, NumberFilterType.GT);
    setDispatchMap(geDispatch, NumberFilterType.GE);
    setDispatchMap(ltDispatch, NumberFilterType.LT);
    setDispatchMap(leDispatch, NumberFilterType.LE);
    setDispatchMap(eqDispatch, NumberFilterType.EQUAL);

    dispatch.put(GreaterThan.class, (f) -> {
      GreaterThan filter = (GreaterThan)f;
      Object value = filter.value();
      OperatorFilterFactory factory = gtDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(GreaterThanOrEqual.class, (f) -> {
      GreaterThanOrEqual filter = (GreaterThanOrEqual)f;
      Object value = filter.value();
      OperatorFilterFactory factory = geDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(LessThan.class, (f) -> {
      LessThan filter = (LessThan)f;
      Object value = filter.value();
      OperatorFilterFactory factory = ltDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(LessThanOrEqual.class, (f) -> {
      LessThanOrEqual filter = (LessThanOrEqual)f;
      Object value = filter.value();
      OperatorFilterFactory factory = leDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(EqualTo.class, (f) -> {
      EqualTo filter = (EqualTo)f;
      Object value = filter.value();
      OperatorFilterFactory factory = eqDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(StringStartsWith.class, (f) -> {
      StringStartsWith filter = (StringStartsWith)f;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new ForwardMatchStringFilter(filter.value()));
    });

    dispatch.put(StringEndsWith.class, (f) -> {
      StringEndsWith filter = (StringEndsWith)f;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new BackwardMatchStringFilter(filter.value()));
    });

    dispatch.put(StringContains.class, (f) -> {
      StringContains filter = (StringContains)f;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new PartialMatchStringFilter(filter.value()));
    });

    dispatch.put(And.class, (f) -> {
      And filter = (And)f;
      IExpressionNode result = new AndExpressionNode();
      result.addChildNode(get(filter.left()));
      result.addChildNode(get(filter.right()));
      return result;
    });

    dispatch.put(Or.class, (f) -> {
      Or filter = (Or)f;
      IExpressionNode result = new OrExpressionNode();
      result.addChildNode(get(filter.left()));
      result.addChildNode(get(filter.right()));
      return result;
    });

    dispatch.put(Not.class, (f) -> {
      Not filter = (Not)f;
      IExpressionNode result = new NotExpressionNode();
      result.addChildNode(get(filter.child()));
      return result;
    });

    dispatch.put(In.class, (f) -> {
      In filter = (In)f;
      Set<String> dic = new HashSet<String>();
      for (Object value : filter.values()) {
        if (!(value instanceof String)) return null;
        dic.add(value.toString());
      }
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new StringDictionaryFilter(dic));
    });

    dispatch.put(EqualNullSafe.class, (f) -> null);
    dispatch.put(IsNull.class,        (f) -> null);
    dispatch.put(IsNotNull.class,     (f) -> null);
  }

  private FilterConnectorFactory() {}

  public static IExpressionNode get(final Filter filter) {
    ExpressionNodeFactory factory = dispatch.get(filter.getClass());
    return (Objects.isNull(factory)) ? null : factory.apply(filter);
  }

  private static void setDispatchMap(Map<Class, OperatorFilterFactory> dispatch, final NumberFilterType type) {
    dispatch.put(Byte.class,    (v) -> new NumberFilter(type, new ByteObj(((Byte)v).byteValue())));
    dispatch.put(Short.class,   (v) -> new NumberFilter(type, new ShortObj(((Short)v).shortValue())));
    dispatch.put(Integer.class, (v) -> new NumberFilter(type, new IntegerObj(((Integer)v).intValue())));
    dispatch.put(Long.class,    (v) -> new NumberFilter(type, new LongObj(((Long)v).longValue())));
    dispatch.put(Float.class,   (v) -> new NumberFilter(type, new FloatObj(((Float)v).floatValue())));
    dispatch.put(Double.class,  (v) -> new NumberFilter(type, new DoubleObj(((Double)v).doubleValue())));
  }

  @FunctionalInterface
  private static interface OperatorFilterFactory {
    IFilter apply(final Object value);
  }

  @FunctionalInterface
  private static interface ExpressionNodeFactory {
    IExpressionNode apply(final Filter filter);
  }
}

