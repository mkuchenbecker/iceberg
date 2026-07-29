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
package org.apache.spark.sql.execution.datasources.v2

import java.util.Collections
import org.apache.iceberg.spark.SupportsViewChanges
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.IcebergAnalysisException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import scala.jdk.CollectionConverters._

case class AlterV2ViewUnsetPropertiesExec(
    catalog: ViewCatalog,
    ident: Identifier,
    propertyKeys: Seq[String],
    ifExists: Boolean)
    extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    if (!ifExists) {
      propertyKeys.filterNot(catalog.loadView(ident).properties.containsKey).foreach { property =>
        throw new IcebergAnalysisException(s"Cannot remove property that is not set: '$property'")
      }
    }

    catalog match {
      case c: SupportsViewChanges =>
        c.alterView(
          ident,
          Collections.emptyMap[String, String](),
          propertyKeys.toSet.asJava)
      case _ =>
        throw new UnsupportedOperationException(
          s"Altering a view is not supported by catalog: ${catalog.name}")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"AlterV2ViewUnsetProperties: ${ident}"
  }
}
