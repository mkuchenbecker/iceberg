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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Pins how native Iceberg column defaults ({@code initial-default} and {@code write-default})
 * behave through Spark 4.2 on the OpenHouse 1.12 line.
 *
 * <p>On this line OpenHouse uses native Iceberg v3 defaults (Parquet first, ahead of ORC) instead
 * of its own read-bridge mechanism. Each test pins one behavior so that a change to it, in the fork
 * or upstream, shows up as a failing test rather than as a silent change in what users read.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestNativeColumnDefaults extends TestBaseWithCatalog {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // DDL: Spark SQL cannot set a default on this line, so OpenHouse users must use the API.

  @TestTemplate
  public void createTableWithDefaultClauseIsRefused() {
    // OpenHouse users get this refusal for CREATE ... DEFAULT until a fork patch adds DDL support.
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s (id bigint, data string DEFAULT 'q') USING iceberg "
                        + "TBLPROPERTIES ('format-version'='3')",
                    tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "[UNSUPPORTED_FEATURE.TABLE_OPERATION] The feature is not supported: Table `"
                + catalogName
                + "`.`default`.`table` does not support column default value.");
    assertThat(validationCatalog.tableExists(tableIdent)).isFalse();
  }

  @TestTemplate
  public void alterTableAddColumnWithDefaultClauseIsRefused() {
    // The DEFAULT clause is refused, not silently dropped as on LinkedIn's 1.5.2 line.
    createV3Table("parquet");

    assertThatThrownBy(() -> sql("ALTER TABLE %s ADD COLUMN c7 int DEFAULT 7", tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Cannot add column c7 since setting default values in Spark is currently unsupported");
    assertThat(table().schema().findField("c7")).isNull();
  }

  @TestTemplate
  public void alterColumnSetDefaultIsRefused() {
    // Changing a write-default is only possible through the API on this line.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    assertThatThrownBy(() -> sql("ALTER TABLE %s ALTER COLUMN country SET DEFAULT 'ZZ'", tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot apply unknown table change: ALTER COLUMN country SET DEFAULT 'ZZ'");
    assertThat(table().schema().findField("country").writeDefault()).isEqualTo("US");
  }

  @TestTemplate
  public void alterColumnDropDefaultIsRefused() {
    // Dropping a write-default is only possible through the API on this line.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    assertThatThrownBy(() -> sql("ALTER TABLE %s ALTER COLUMN country DROP DEFAULT", tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot apply unknown table change: ALTER COLUMN country DROP DEFAULT");
    assertThat(table().schema().findField("country").writeDefault()).isEqualTo("US");
  }

  // API: addColumn with a default sets both defaults; old files read the initial default.

  @TestTemplate
  public void apiAddColumnWithDefaultSetsInitialAndWriteDefault() {
    // OpenHouse stores exactly these two values in table metadata; the server validates them.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    Types.NestedField country = table().schema().findField("country");
    assertThat(country.initialDefault()).isEqualTo("US");
    assertThat(country.writeDefault()).isEqualTo("US");
    assertThat(currentMetadataJson())
        .contains(
            "\"name\":\"country\",\"required\":false,\"type\":\"string\","
                + "\"initial-default\":\"US\",\"write-default\":\"US\"");
  }

  @TestTemplate
  public void parquetRowReaderFillsInitialDefaultForOldFiles() {
    // Parquet is the target format for native defaults on OpenHouse's new line.
    assertOldRowsReadInitialDefault("parquet", false);
  }

  @TestTemplate
  public void parquetVectorizedReaderFillsInitialDefaultForOldFiles() {
    // Most OpenHouse Spark reads of Parquet are vectorized.
    assertOldRowsReadInitialDefault("parquet", true);
  }

  @TestTemplate
  public void avroReaderFillsInitialDefaultForOldFiles() {
    // Avro needs no fork patch to fill defaults.
    assertOldRowsReadInitialDefault("avro", false);
  }

  @TestTemplate
  public void orcRowReaderFillsInitialDefaultForOldFiles() {
    // The Spark 4.2 port of the LinkedIn ORC patches opts in to ORC default filling, like 3.5.
    assertOldRowsReadInitialDefault("orc", false);
  }

  @TestTemplate
  public void orcVectorizedScanFallsBackAndFillsInitialDefaultForOldFiles() {
    // Batched ORC cannot fill defaults; Spark keeps defaulted projections on the row reader.
    assertOldRowsReadInitialDefault("orc", true);
  }

  @TestTemplate
  public void parquetReaderFillsNestedFieldDefaultForOldFiles() {
    // Defaults on struct fields must work for OpenHouse tables with nested schemas.
    assertOldRowsReadNestedDefault("parquet");
  }

  @TestTemplate
  public void avroReaderFillsNestedFieldDefaultForOldFiles() {
    // Defaults on struct fields must work for OpenHouse tables with nested schemas.
    assertOldRowsReadNestedDefault("avro");
  }

  @TestTemplate
  public void orcReaderFillsNestedFieldDefaultForOldFiles() {
    // Defaults on struct fields must work for OpenHouse tables with nested schemas.
    assertOldRowsReadNestedDefault("orc");
  }

  @TestTemplate
  public void apiUpdateColumnDefaultChangesOnlyWriteDefault() {
    // initial-default is immutable; a default change must never rewrite what old rows read.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    table().updateSchema().updateColumnDefault("country", Literal.of("XX")).commit();

    Types.NestedField country = table().schema().findField("country");
    assertThat(country.initialDefault()).isEqualTo("US");
    assertThat(country.writeDefault()).isEqualTo("XX");
    assertEquals(
        "Old rows keep reading the initial default",
        ImmutableList.of(row(1L, "US"), row(2L, "CA"), row(3L, null)),
        sql("SELECT id, country FROM %s ORDER BY id", tableName));
  }

  // Writes: Spark 4.2 sees Iceberg write-defaults (Spark 3.5 does not; it writes NULL).

  @TestTemplate
  public void insertWithDefaultKeywordWritesWriteDefault() {
    // On Spark 4.2 the DEFAULT keyword writes the write-default; Spark 3.5 writes NULL.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    sql("INSERT INTO %s VALUES (4, 'd', DEFAULT)", tableName);

    assertEquals(
        "DEFAULT keyword writes the write-default on Spark 4.2",
        ImmutableList.of(row(4L, "US")),
        sql("SELECT id, country FROM %s WHERE id = 4", tableName));
  }

  @TestTemplate
  public void insertWithDefaultKeywordFollowsChangedWriteDefault() {
    // After an API write-default change, Spark 4.2 writes the new value; old rows keep theirs.
    createV3TableWithOldRowAndCountryDefault("parquet", false);
    table().updateSchema().updateColumnDefault("country", Literal.of("XX")).commit();

    sql("INSERT INTO %s VALUES (4, 'd', DEFAULT)", tableName);

    assertEquals(
        "DEFAULT keyword writes the changed write-default on Spark 4.2",
        ImmutableList.of(row(1L, "US"), row(4L, "XX")),
        sql("SELECT id, country FROM %s WHERE id IN (1, 4) ORDER BY id", tableName));
  }

  @TestTemplate
  public void insertOmittingDefaultedColumnWritesWriteDefault() {
    // On Spark 4.2 a column list that omits a defaulted column writes its write-default.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    sql("INSERT INTO %s (id, name) VALUES (4, 'd')", tableName);

    assertEquals(
        "Omitted column takes the write-default on Spark 4.2",
        ImmutableList.of(row(4L, "US")),
        sql("SELECT id, country FROM %s WHERE id = 4", tableName));
  }

  @TestTemplate
  public void insertOmittingColumnWithoutWriteDefaultWritesNull() {
    // With no write-default (the migrated-table shape), an omitted column is written as NULL.
    createV3TableWithOldRowAndCountryDefault("parquet", false);
    table().updateSchema().updateColumnDefault("country", null).commit();

    sql("INSERT INTO %s (id, name) VALUES (4, 'd')", tableName);

    assertEquals(
        "Omitted column without a write-default is NULL on Spark 4.2",
        ImmutableList.of(row(1L, "US"), row(4L, null)),
        sql("SELECT id, country FROM %s WHERE id IN (1, 4) ORDER BY id", tableName));
  }

  // Initial default without a write default: the shape OpenHouse's migration (W8) writes.

  @TestTemplate
  public void apiUpdateColumnDefaultToNullDropsOnlyWriteDefault() {
    // Migrated bridged tables keep their initial-default and get no write-default, as legacy did.
    createV3TableWithOldRowAndCountryDefault("parquet", false);

    table().updateSchema().updateColumnDefault("country", null).commit();

    Types.NestedField country = table().schema().findField("country");
    assertThat(country.initialDefault()).isEqualTo("US");
    assertThat(country.writeDefault()).isNull();
    assertThat(currentMetadataJson())
        .contains(
            "\"name\":\"country\",\"required\":false,\"type\":\"string\",\"initial-default\":\"US\"}");
  }

  @TestTemplate
  public void initialDefaultWithoutWriteDefaultRoundTripsThroughReadAndWrite() {
    // Old rows keep the initial default; new writes store what Spark writes; metadata is unchanged.
    for (String format : new String[] {"parquet", "orc", "avro"}) {
      sql("DROP TABLE IF EXISTS %s", tableName);
      createV3TableWithOldRowAndCountryDefault(format, false);
      table().updateSchema().updateColumnDefault("country", null).commit();

      sql("INSERT INTO %s VALUES (4, 'd', DEFAULT), (5, 'e', 'MX')", tableName);

      assertEquals(
          format + ": old rows read the initial default, DEFAULT writes NULL",
          ImmutableList.of(
              row(1L, "US"), row(2L, "CA"), row(3L, null), row(4L, null), row(5L, "MX")),
          sql("SELECT id, country FROM %s ORDER BY id", tableName));
      assertEquals(
          format + ": filter on the initial default returns the old row",
          ImmutableList.of(row(1L)),
          sql("SELECT id FROM %s WHERE country = 'US' ORDER BY id", tableName));
      Types.NestedField country = table().schema().findField("country");
      assertThat(country.initialDefault()).as(format).isEqualTo("US");
      assertThat(country.writeDefault()).as(format).isNull();
    }
  }

  // Format v2: native defaults need v3.

  @TestTemplate
  public void v2AddColumnWithInitialDefaultIsRefusedClientSide() {
    // Iceberg itself refuses a non-null initial-default below v3, before OpenHouse sees a commit.
    createTable(2, "parquet", false);

    assertThatThrownBy(
            () ->
                table()
                    .updateSchema()
                    .addColumn("country", Types.StringType.get(), Literal.of("US"))
                    .commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v2:\n"
                + "- Invalid initial default for country: "
                + "non-null default (US) is not supported until v3");
    assertThat(table().schema().findField("country")).isNull();
  }

  @TestTemplate
  public void v2UpdateColumnDefaultPersistsWriteDefault() {
    // Iceberg accepts a write-default on v2 today; the OpenHouse service must refuse this (W5).
    createTable(2, "parquet", false);
    table().updateSchema().addColumn("c2", Types.StringType.get()).commit();

    table().updateSchema().updateColumnDefault("c2", Literal.of("W")).commit();

    assertThat(formatVersion()).isEqualTo(2);
    Types.NestedField c2 = table().schema().findField("c2");
    assertThat(c2.initialDefault()).isNull();
    assertThat(c2.writeDefault()).isEqualTo("W");
    assertThat(currentMetadataJson())
        .contains(
            "\"name\":\"c2\",\"required\":false,\"type\":\"string\",\"write-default\":\"W\"}");
    assertEquals(
        "The v2 write-default does not change reads",
        ImmutableList.of(row(1L, null)),
        sql("SELECT id, c2 FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void formatVersionUpgradeToV3CannotBeReversed() {
    // Upgrading is a one-way door: legacy readers cannot load v3 metadata afterwards.
    createTable(2, "parquet", false);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='3')", tableName);
    assertThat(formatVersion()).isEqualTo(3);

    assertThatThrownBy(
            () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='2')", tableName))
        .isInstanceOf(SparkException.class)
        .hasMessageContaining("Cannot downgrade v3 table to v2");
    assertThat(formatVersion()).isEqualTo(3);
  }

  @TestTemplate
  public void upgradedTableReadsLaterAddedDefaultForOldV2Rows() {
    // After an upgrade, answers for old v2 Parquet rows change from NULL to the new default.
    createTable(2, "parquet", false);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='3')", tableName);

    table().updateSchema().addColumn("country", Types.StringType.get(), Literal.of("US")).commit();

    assertEquals(
        "Rows written while the table was v2 read the default",
        ImmutableList.of(row(1L, "US")),
        sql("SELECT id, country FROM %s ORDER BY id", tableName));
  }

  private void assertOldRowsReadInitialDefault(String format, boolean vectorized) {
    createV3TableWithOldRowAndCountryDefault(format, vectorized);

    assertEquals(
        format + ": old row reads the initial default, explicit values are kept",
        ImmutableList.of(row(1L, "US"), row(2L, "CA"), row(3L, null)),
        sql("SELECT id, country FROM %s ORDER BY id", tableName));
    assertEquals(
        format + ": filter on the default returns the old row",
        ImmutableList.of(row(1L)),
        sql("SELECT id FROM %s WHERE country = 'US' ORDER BY id", tableName));
    assertEquals(
        format + ": IS NULL returns only the explicit NULL",
        ImmutableList.of(row(3L)),
        sql("SELECT id FROM %s WHERE country IS NULL ORDER BY id", tableName));
  }

  private void assertOldRowsReadNestedDefault(String format) {
    sql(
        "CREATE TABLE %s (id bigint, loc struct<zip:string>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.format.default'='%s')",
        tableName, format);
    sql("INSERT INTO %s VALUES (1, named_struct('zip', '1'))", tableName);

    table()
        .updateSchema()
        .addColumn("loc", "city", Types.StringType.get(), Literal.of("X"))
        .commit();
    sql("INSERT INTO %s VALUES (2, named_struct('zip', '2', 'city', 'Y'))", tableName);

    assertThat(table().schema().findField("loc.city").initialDefault()).isEqualTo("X");
    assertEquals(
        format + ": old row reads the nested initial default",
        ImmutableList.of(row(1L, "1", "X"), row(2L, "2", "Y")),
        sql("SELECT id, loc.zip, loc.city FROM %s ORDER BY id", tableName));
    assertEquals(
        format + ": filter on the nested default returns the old row",
        ImmutableList.of(row(1L)),
        sql("SELECT id FROM %s WHERE loc.city = 'X' ORDER BY id", tableName));
  }

  /** Creates a v3 table with row 1 written before {@code country} (default US) was added. */
  private void createV3TableWithOldRowAndCountryDefault(String format, boolean vectorized) {
    createTable(3, format, vectorized);
    table().updateSchema().addColumn("country", Types.StringType.get(), Literal.of("US")).commit();
    sql("INSERT INTO %s VALUES (2, 'b', 'CA'), (3, 'c', NULL)", tableName);
  }

  private void createV3Table(String format) {
    createTable(3, format, false);
  }

  private void createTable(int formatVersion, String format, boolean vectorized) {
    sql(
        "CREATE TABLE %s (id bigint, name string) USING iceberg TBLPROPERTIES ("
            + "'format-version'='%d', 'write.format.default'='%s', "
            + "'read.parquet.vectorization.enabled'='%b', 'read.orc.vectorization.enabled'='%b')",
        tableName, formatVersion, format, vectorized, vectorized);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
  }

  private Table table() {
    return validationCatalog.loadTable(tableIdent);
  }

  private TableMetadata currentMetadata() {
    return ((HasTableOperations) table()).operations().current();
  }

  private int formatVersion() {
    return currentMetadata().formatVersion();
  }

  private String currentMetadataJson() {
    return TableMetadataParser.toJson(currentMetadata());
  }
}
