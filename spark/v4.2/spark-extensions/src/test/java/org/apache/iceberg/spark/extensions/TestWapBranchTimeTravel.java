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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Reads of a write-audit-publish table while the session has {@code spark.wap.branch} set.
 *
 * <p>An explicit snapshot, timestamp, tag or branch in the query must win over the session WAP
 * branch; a read without one follows the WAP branch once it exists. Each read is checked twice:
 * selecting rows (a normal batch scan) and {@code count(*)} (answered by aggregate push-down).
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestWapBranchTimeTravel extends ExtensionsTestBase {

  private static final String WAP_BRANCH = "wap";
  private static final String TAG = "t1";
  private static final String OTHER_BRANCH = "other";

  private long firstSnapshotId;
  private String firstSnapshotTimestamp;
  private long firstSnapshotMillis;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @BeforeEach
  public void createTable() throws InterruptedException {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);

    // main: snapshot 1 holds id 1, snapshot 2 (main head) adds id 2
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot first = table.currentSnapshot();
    this.firstSnapshotId = first.snapshotId();
    this.firstSnapshotMillis = first.timestampMillis();
    this.firstSnapshotTimestamp =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(firstSnapshotMillis));

    waitUntilAfter(firstSnapshotMillis);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    // tag t1 on snapshot 1; branch other from snapshot 1 with its own commit adding id 9
    table.refresh();
    table.manageSnapshots().createTag(TAG, firstSnapshotId).commit();
    table.manageSnapshots().createBranch(OTHER_BRANCH, firstSnapshotId).commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO %s.branch_%s VALUES (9, 'z')", tableName, OTHER_BRANCH);
    sql("REFRESH TABLE %s", tableName);
  }

  @AfterEach
  public void removeTable() {
    spark.conf().unset(SparkSQLProperties.WAP_BRANCH);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testWapBranchSetButNotCreated() {
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, WAP_BRANCH);

    assertThat(observe()).containsExactlyEntriesOf(expected("[1, 2]"));
  }

  @TestTemplate
  public void testWapBranchAtMainHead() {
    Table table = validationCatalog.loadTable(tableIdent);
    table.manageSnapshots().createBranch(WAP_BRANCH, table.currentSnapshot().snapshotId()).commit();
    sql("REFRESH TABLE %s", tableName);
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, WAP_BRANCH);

    assertThat(observe()).containsExactlyEntriesOf(expected("[1, 2]"));
  }

  @TestTemplate
  public void testWapBranchWithStagedWrite() {
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, WAP_BRANCH);
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);

    // the staged write went to the WAP branch only
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.refs()).containsKey(WAP_BRANCH);
    assertThat(table.snapshot(WAP_BRANCH).parentId())
        .isEqualTo(table.currentSnapshot().snapshotId());

    assertThat(observe()).containsExactlyEntriesOf(expected("[1, 2, 3]"));
  }

  /** Every read in the matrix: explicit reads see main history or the named ref. */
  private Map<String, String> expected(String unversionedIds) {
    Map<String, String> expected = Maps.newLinkedHashMap();
    expect(expected, "no version", unversionedIds);
    expect(expected, "VERSION AS OF <snapshot 1>", "[1]");
    expect(expected, "TIMESTAMP AS OF <snapshot 1>", "[1]");
    expect(expected, "VERSION AS OF '" + TAG + "'", "[1]");
    expect(expected, "VERSION AS OF '" + OTHER_BRANCH + "'", "[1, 9]");
    expect(expected, "identifier branch_" + OTHER_BRANCH, "[1, 9]");
    expect(expected, "option versionAsOf <snapshot 1>", "[1]");
    expect(expected, "option timestampAsOf <snapshot 1>", "[1]");
    expect(expected, "option versionAsOf '" + TAG + "'", "[1]");
    expect(expected, "option branch", "[1, 9]");
    // Spark 4.1+ rejects the legacy Iceberg time-travel options, with or without WAP
    expectError(expected, "option snapshot-id", SparkReadOptions.LEGACY_SNAPSHOT_ID);
    expectError(expected, "option as-of-timestamp", SparkReadOptions.LEGACY_AS_OF_TIMESTAMP);
    expectError(expected, "option tag", SparkReadOptions.LEGACY_TAG);
    return expected;
  }

  private static void expectError(Map<String, String> expected, String read, String option) {
    String error = "error: IllegalArgumentException: Time travel option `" + option + "`";
    expected.put(read + " / rows", error);
    expected.put(read + " / count", error);
  }

  private static void expect(Map<String, String> expected, String read, String ids) {
    int count = ids.equals("[]") ? 0 : ids.split(",").length;
    expected.put(read + " / rows", ids);
    expected.put(read + " / count", String.valueOf(count));
  }

  private Map<String, String> observe() {
    Map<String, String> actual = Maps.newLinkedHashMap();
    observeSql(actual, "no version", tableName);
    observeSql(
        actual, "VERSION AS OF <snapshot 1>", tableName + " VERSION AS OF " + firstSnapshotId);
    observeSql(
        actual,
        "TIMESTAMP AS OF <snapshot 1>",
        tableName + " TIMESTAMP AS OF '" + firstSnapshotTimestamp + "'");
    observeSql(actual, "VERSION AS OF '" + TAG + "'", tableName + " VERSION AS OF '" + TAG + "'");
    observeSql(
        actual,
        "VERSION AS OF '" + OTHER_BRANCH + "'",
        tableName + " VERSION AS OF '" + OTHER_BRANCH + "'");
    observeSql(actual, "identifier branch_" + OTHER_BRANCH, tableName + ".branch_" + OTHER_BRANCH);
    observeOption(
        actual, "option versionAsOf <snapshot 1>", SparkReadOptions.VERSION_AS_OF, firstSnapshotId);
    observeOption(
        actual,
        "option timestampAsOf <snapshot 1>",
        SparkReadOptions.TIMESTAMP_AS_OF,
        firstSnapshotTimestamp);
    observeOption(actual, "option versionAsOf '" + TAG + "'", SparkReadOptions.VERSION_AS_OF, TAG);
    observeOption(actual, "option branch", SparkReadOptions.BRANCH, OTHER_BRANCH);
    observeOptionError(
        actual, "option snapshot-id", SparkReadOptions.LEGACY_SNAPSHOT_ID, firstSnapshotId);
    observeOptionError(
        actual,
        "option as-of-timestamp",
        SparkReadOptions.LEGACY_AS_OF_TIMESTAMP,
        firstSnapshotMillis);
    observeOptionError(actual, "option tag", SparkReadOptions.LEGACY_TAG, TAG);
    return actual;
  }

  // keeps only the stable start of the message, which goes on to name the replacement option
  private void observeOptionError(
      Map<String, String> actual, String read, String option, Object value) {
    observeOption(actual, read, option, value);
    String prefix = "error: IllegalArgumentException: Time travel option `" + option + "`";
    actual.replaceAll((key, outcome) -> outcome.startsWith(prefix) ? prefix : outcome);
  }

  private void observeSql(Map<String, String> actual, String read, String target) {
    actual.put(
        read + " / rows",
        outcome(() -> ids(spark.sql("SELECT id FROM " + target + " ORDER BY id"))));
    actual.put(
        read + " / count", outcome(() -> count(spark.sql("SELECT count(*) FROM " + target))));
  }

  private void observeOption(Map<String, String> actual, String read, String option, Object value) {
    actual.put(
        read + " / rows",
        outcome(() -> ids(readWithOption(option, value).select("id").orderBy("id"))));
    actual.put(
        read + " / count",
        outcome(() -> count(readWithOption(option, value).selectExpr("count(*)"))));
  }

  private Dataset<Row> readWithOption(String option, Object value) {
    return spark.read().format("iceberg").option(option, String.valueOf(value)).load(tableName);
  }

  private static String ids(Dataset<Row> df) {
    List<Long> ids =
        df.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
    return ids.toString();
  }

  private static String count(Dataset<Row> df) {
    return String.valueOf(df.collectAsList().get(0).getLong(0));
  }

  private static String outcome(Supplier<String> read) {
    try {
      return read.get();
    } catch (RuntimeException e) {
      return "error: " + e.getClass().getSimpleName() + ": " + e.getMessage();
    }
  }
}
