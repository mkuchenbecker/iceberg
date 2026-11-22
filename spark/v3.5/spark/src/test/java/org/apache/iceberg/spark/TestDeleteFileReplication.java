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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end SQL tests for delete file replication.
 *
 * <p>The catalog warehouse lives on a dedicated {@code capturefs:} filesystem that records the
 * replication factor passed to every stream creation, which is what HDFS would receive in
 * production. Merge-on-read deletes produce the delete files that are inspected.
 *
 * <p>Delete files are written as Avro here: the Parquet writer opens Hadoop output files by path
 * and does not go through {@link org.apache.iceberg.hadoop.HadoopOutputFile#create()}.
 */
public class TestDeleteFileReplication extends TestBase {

  private static final String CATALOG = "repltestcat";
  private static final String TABLE = CATALOG + ".default.repl_test";

  @BeforeAll
  public static void setUpCatalog() throws IOException {
    // session confs are copied into sessionState().newHadoopConf(), which SparkCatalog uses
    spark.conf().set("fs.capturefs.impl", CaptureReplicationFileSystem.class.getName());
    spark.conf().set("fs.capturefs.impl.disable.cache", "true");

    String warehouse = Files.createTempDirectory("repl-warehouse").toString();
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", "capturefs:" + warehouse);
  }

  @AfterEach
  public void cleanUp() {
    spark.conf().unset(SparkSQLProperties.DELETE_FILE_REPLICATION);
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro"})
  public void testDeleteFileReplicationFromTableProperty(String deleteFormat) {
    createTable(deleteFormat, ", 'write.delete-file-replication'='5'");
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", TABLE);
    sql("DELETE FROM %s WHERE id IN (1, 2)", TABLE);

    assertDeleteFileReplication(deleteFormat, (short) 5);
    assertDataFilesUseDefaultReplication();
    assertThat(sql("SELECT id FROM %s ORDER BY id", TABLE))
        .extracting(row -> row[0])
        .containsExactly(3, 4, 5);
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro"})
  public void testDeleteFileReplicationFromSessionConf(String deleteFormat) {
    createTable(deleteFormat, "");
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", TABLE);

    // the key contains hyphens, so the Spark SQL parser requires backquotes
    sql("SET `%s`=7", SparkSQLProperties.DELETE_FILE_REPLICATION);
    sql("DELETE FROM %s WHERE id IN (1, 2)", TABLE);

    assertDeleteFileReplication(deleteFormat, (short) 7);
    assertDataFilesUseDefaultReplication();
    assertThat(sql("SELECT id FROM %s ORDER BY id", TABLE))
        .extracting(row -> row[0])
        .containsExactly(3, 4, 5);
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro"})
  public void testDeleteFilesUseFileSystemDefaultReplicationWhenNotConfigured(String deleteFormat) {
    createTable(deleteFormat, "");
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", TABLE);
    sql("DELETE FROM %s WHERE id IN (1, 2)", TABLE);

    List<Object[]> deleteFiles = sql("SELECT file_path FROM %s.delete_files", TABLE);
    assertThat(deleteFiles).isNotEmpty();
    for (Object[] row : deleteFiles) {
      String location = (String) row[0];
      assertThat(CaptureReplicationFileSystem.capturedReplication(location))
          .as("Delete file %s should keep the filesystem default replication", location)
          .isEqualTo(defaultReplication(location));
    }
  }

  private void createTable(String deleteFormat, String extraProperties) {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='2', "
            + "'write.delete.mode'='merge-on-read', "
            + "'write.delete.format.default'='%s'%s)",
        TABLE, deleteFormat, extraProperties);
  }

  private void assertDeleteFileReplication(String deleteFormat, short expected) {
    List<Object[]> deleteFiles = sql("SELECT file_path FROM %s.delete_files", TABLE);
    assertThat(deleteFiles).isNotEmpty();
    for (Object[] row : deleteFiles) {
      String location = (String) row[0];
      assertThat(location).endsWith("." + deleteFormat);
      assertThat(CaptureReplicationFileSystem.capturedReplication(location))
          .as("Delete file %s should be created with the configured replication", location)
          .isEqualTo(expected);
    }
  }

  private void assertDataFilesUseDefaultReplication() {
    List<Object[]> dataFiles = sql("SELECT file_path FROM %s.files WHERE content = 0", TABLE);
    assertThat(dataFiles).isNotEmpty();
    for (Object[] row : dataFiles) {
      String location = (String) row[0];
      assertThat(CaptureReplicationFileSystem.capturedReplication(location))
          .as("Data file %s should keep the filesystem default replication", location)
          .isEqualTo(defaultReplication(location));
    }
  }

  private static short defaultReplication(String location) {
    try {
      Path path = new Path(location);
      return path.getFileSystem(spark.sessionState().newHadoopConf()).getDefaultReplication(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Local filesystem under a dedicated scheme that records the replication of each stream. */
  public static class CaptureReplicationFileSystem extends RawLocalFileSystem {
    private static final Map<String, Short> CAPTURED = Maps.newConcurrentMap();

    static short capturedReplication(String location) {
      String name = new Path(location).getName();
      Short replication = CAPTURED.get(name);
      assertThat(replication).as("No stream was created for %s", location).isNotNull();
      return replication;
    }

    @Override
    public String getScheme() {
      return "capturefs";
    }

    @Override
    public URI getUri() {
      return URI.create("capturefs:///");
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      CAPTURED.put(path.getName(), replication);
      return super.create(path, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      CAPTURED.put(path.getName(), replication);
      return super.create(
          path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }
  }
}
