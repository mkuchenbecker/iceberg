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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.ViewUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._

case class CreateV2ViewExec(
    catalog: ViewCatalog,
    ident: Identifier,
    queryText: String,
    viewSchema: StructType,
    columnAliases: Seq[String],
    columnComments: Seq[Option[String]],
    queryColumnNames: Seq[String],
    comment: Option[String],
    properties: Map[String, String],
    allowExisting: Boolean,
    replace: Boolean)
    extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val currentCatalogName = session.sessionState.catalogManager.currentCatalog.name
    val currentCatalog =
      if (!catalog.name().equals(currentCatalogName)) currentCatalogName else null
    val currentNamespace = session.sessionState.catalogManager.currentNamespace

    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    val newProperties = properties ++
      comment.map(ViewUtil.PROP_COMMENT -> _) ++
      Map(
        ViewUtil.PROP_CREATE_ENGINE_VERSION -> engineVersion,
        ViewUtil.PROP_ENGINE_VERSION -> engineVersion)

    val view = buildView(currentCatalog, currentNamespace, newProperties)

    if (replace) {
      // CREATE OR REPLACE VIEW
      catalog.createOrReplaceView(ident, view)
    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        catalog.createView(ident, view)
      } catch {
        case _: ViewAlreadyExistsException if allowExisting => // Ignore
      }
    }

    Nil
  }

  private def buildView(
      currentCatalog: String,
      currentNamespace: Array[String],
      newProperties: Map[String, String]): View = {
    // Use statement-per-setter because the inherited RelationBuilder setters are declared in a
    // package-private class whose static return type cannot be named from this package.
    val builder = new View.Builder()
    builder.withSchema(viewSchema)
    builder.withProperties(newProperties.asJava)
    builder.withQueryText(queryText)
    builder.withCurrentCatalog(currentCatalog)
    builder.withCurrentNamespace(currentNamespace)
    builder.withQueryColumnNames(queryColumnNames.toArray)
    comment.foreach { c =>
      builder.withComment(c)
      ()
    }
    builder.build()
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateV2ViewExec: ${ident}"
  }
}
