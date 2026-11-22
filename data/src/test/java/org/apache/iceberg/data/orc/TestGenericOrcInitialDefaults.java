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
package org.apache.iceberg.data.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The generic ORC reader does not fill {@code initial-default} values. Reading an old ORC file that
 * lacks a column with a non-null initial default must fail, as on Apache Iceberg, rather than
 * return NULL for a value the table says is the default.
 */
public class TestGenericOrcInitialDefaults {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));

  @TempDir private File tableDir;
  @TempDir private java.nio.file.Path temp;

  @Test
  public void genericOrcReadOfOldFileMissingDefaultedColumnFails() throws IOException {
    // A silent NULL here would differ from what Spark 3.5 and Parquet readers return for the row.
    Table table = createV3OrcTableWithOldFile();
    table.updateSchema().addColumn("country", Types.StringType.get(), Literal.of("US")).commit();

    assertThatThrownBy(() -> readIdAndCountry(table))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("ORC cannot read default value for field country (string): US");
  }

  @Test
  public void genericOrcReadOfOldFileMissingOptionalColumnWithoutDefaultReadsNull()
      throws IOException {
    // Columns added without a default must keep reading NULL from old ORC files.
    Table table = createV3OrcTableWithOldFile();
    table.updateSchema().addColumn("country", Types.StringType.get()).commit();

    assertThat(readIdAndCountry(table)).containsExactly(Lists.newArrayList(1L, null));
  }

  @Test
  public void genericOrcReadOfFileContainingDefaultedColumnReadsStoredValues() throws IOException {
    // Files written after the column was added carry it, so the reader never needs the default.
    Table table = createV3OrcTable();
    table.updateSchema().addColumn("country", Types.StringType.get(), Literal.of("US")).commit();

    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2L);
    record.setField("name", "b");
    record.setField("country", "CA");
    new GenericAppenderHelper(table, FileFormat.ORC, temp).appendToTable(ImmutableList.of(record));

    assertThat(readIdAndCountry(table)).containsExactly(Lists.newArrayList(2L, "CA"));
  }

  private Table createV3OrcTableWithOldFile() throws IOException {
    Table table = createV3OrcTable();
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", 1L);
    record.setField("name", "a");
    new GenericAppenderHelper(table, FileFormat.ORC, temp).appendToTable(ImmutableList.of(record));
    return table;
  }

  private Table createV3OrcTable() {
    return new HadoopTables(new Configuration())
        .create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "3", TableProperties.DEFAULT_FILE_FORMAT, "orc"),
            tableDir.toURI().toString());
  }

  private static List<List<Object>> readIdAndCountry(Table table) throws IOException {
    List<List<Object>> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).select("id", "country").build()) {
      for (Record record : records) {
        rows.add(Lists.newArrayList(record.getField("id"), record.getField("country")));
      }
    }

    return rows;
  }
}
