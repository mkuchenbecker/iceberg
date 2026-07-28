/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewOperations;
import org.apache.spark.sql.types.StructType;

/**
 * Helper that adapts an Iceberg {@link View} to Spark's connector {@code View} representation.
 *
 * <p>In Spark 4.2 {@code org.apache.spark.sql.connector.catalog.View} is a concrete class built via
 * {@code View.Builder} instead of an interface, so this class no longer implements it. Instead it
 * exposes {@link #asSparkView(String, View)} to build a Spark {@code View} from an Iceberg view.
 */
public class SparkView {

  public static final String QUERY_COLUMN_NAMES = "spark.query-column-names";
  public static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of("provider", "location", FORMAT_VERSION, QUERY_COLUMN_NAMES);

  private SparkView() {}

  /** Builds a Spark connector {@link org.apache.spark.sql.connector.catalog.View} from an Iceberg view. */
  public static org.apache.spark.sql.connector.catalog.View asSparkView(
      String catalogName, View icebergView) {
    SQLViewRepresentation sqlRepr = icebergView.sqlFor("spark");
    Preconditions.checkState(sqlRepr != null, "Cannot load SQL for view %s", icebergView.name());

    String currentCatalog =
        icebergView.currentVersion().defaultCatalog() != null
            ? icebergView.currentVersion().defaultCatalog()
            : catalogName;
    String[] currentNamespace = icebergView.currentVersion().defaultNamespace().levels();
    String[] queryColumnNames =
        icebergView.properties().containsKey(QUERY_COLUMN_NAMES)
            ? icebergView.properties().get(QUERY_COLUMN_NAMES).split(",")
            : new String[0];
    StructType schema = SparkSchemaUtil.convert(icebergView.schema());

    // Use statement-per-setter (rather than a fluent chain) because the inherited RelationBuilder
    // setters are declared in a package-private class and their static return type is not nameable
    // from this package; mutating the builder in place avoids referencing that type.
    org.apache.spark.sql.connector.catalog.View.Builder builder =
        new org.apache.spark.sql.connector.catalog.View.Builder();
    builder.withSchema(schema);
    builder.withProperties(properties(icebergView));
    builder.withQueryText(sqlRepr.sql());
    builder.withCurrentCatalog(currentCatalog);
    builder.withCurrentNamespace(currentNamespace);
    builder.withQueryColumnNames(queryColumnNames);
    return builder.build();
  }

  private static Map<String, String> properties(View icebergView) {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    propsBuilder.put("provider", "iceberg");
    propsBuilder.put("location", icebergView.location());

    if (icebergView instanceof BaseView) {
      ViewOperations ops = ((BaseView) icebergView).operations();
      propsBuilder.put(FORMAT_VERSION, String.valueOf(ops.current().formatVersion()));
    }

    icebergView.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }
}
