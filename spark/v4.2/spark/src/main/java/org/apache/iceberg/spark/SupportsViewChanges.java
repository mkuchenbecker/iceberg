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
package org.apache.iceberg.spark;

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;

/**
 * An Iceberg extension to Spark's {@link ViewCatalog} for altering view properties.
 *
 * <p>Spark 4.2 removed {@code ViewChange} and {@code ViewCatalog.alterView}, so Iceberg exposes
 * view property changes through this interface instead.
 */
public interface SupportsViewChanges extends ViewCatalog {
  /**
   * Alter a view by setting and/or removing properties.
   *
   * @param ident a view identifier
   * @param setProperties properties to set on the view
   * @param removeProperties property keys to remove from the view
   * @return the altered view
   * @throws NoSuchViewException if the view does not exist
   */
  View alterView(
      Identifier ident, Map<String, String> setProperties, Set<String> removeProperties)
      throws NoSuchViewException;
}
