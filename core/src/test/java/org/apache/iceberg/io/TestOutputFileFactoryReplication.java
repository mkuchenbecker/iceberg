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
package org.apache.iceberg.io;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.ReplicationCapturingFileSystem;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOutputFileFactoryReplication {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;
  private Configuration conf;
  private Table table;

  @BeforeEach
  public void before() {
    this.conf = ReplicationCapturingFileSystem.newConf();
    this.table =
        new HadoopTables(conf)
            .create(SCHEMA, PartitionSpec.unpartitioned(), new File(tempDir, "t").toString());
  }

  private static void write(OutputFile file) throws IOException {
    try (PositionOutputStream out = file.create()) {
      out.write("data".getBytes());
    }
  }

  private short fsDefaultReplication(OutputFile file) throws IOException {
    Path path = new Path(file.location());
    return path.getFileSystem(conf).getDefaultReplication(path);
  }

  private static String fileName(OutputFile file) {
    return file.location().substring(file.location().lastIndexOf('/') + 1);
  }

  @Test
  public void testReplicationFactorAppliedToOutputFiles() throws IOException {
    OutputFileFactory factory =
        OutputFileFactory.builderFor(table, 1, 100L)
            .operationId("deletes")
            .replicationFactor((short) 5)
            .build();

    OutputFile file = factory.newOutputFile().encryptingOutputFile();
    write(file);

    assertThat(ReplicationCapturingFileSystem.capturedReplication(fileName(file)))
        .isEqualTo((short) 5);
  }

  @Test
  public void testReplicationFactorAppliedToPartitionedOutputFiles() throws IOException {
    OutputFileFactory factory =
        OutputFileFactory.builderFor(table, 2, 100L)
            .operationId("deletes")
            .replicationFactor((short) 4)
            .build();

    OutputFile file =
        factory.newOutputFile(PartitionSpec.unpartitioned(), null).encryptingOutputFile();
    write(file);

    assertThat(ReplicationCapturingFileSystem.capturedReplication(fileName(file)))
        .isEqualTo((short) 4);
  }

  @Test
  public void testDefaultReplicationWhenNotConfigured() throws IOException {
    OutputFileFactory factory =
        OutputFileFactory.builderFor(table, 3, 100L).operationId("default").build();

    OutputFile file = factory.newOutputFile().encryptingOutputFile();
    write(file);

    assertThat(ReplicationCapturingFileSystem.capturedReplication(fileName(file)))
        .isEqualTo(fsDefaultReplication(file));
  }

  @Test
  public void testNonPositiveReplicationFactorIsIgnored() throws IOException {
    for (short replication : new short[] {0, -1}) {
      Map<String, String> seen = Maps.newHashMap();
      FileIO recording = new RecordingFileIO(seen);
      OutputFileFactory factory =
          OutputFileFactory.builderFor(table, 4, 100L)
              .ioSupplier(() -> recording)
              .replicationFactor(replication)
              .build();

      factory.newOutputFile();

      assertThat(seen).containsEntry("plain", "true").doesNotContainKey("props");
    }
  }

  @Test
  public void testReplicationFactorPropertyPassedToFileIO() {
    Map<String, String> seen = Maps.newHashMap();
    OutputFileFactory factory =
        OutputFileFactory.builderFor(table, 5, 100L)
            .ioSupplier(() -> new RecordingFileIO(seen))
            .replicationFactor((short) 3)
            .build();

    factory.newOutputFile();

    assertThat(seen)
        .containsEntry("props", "{" + OutputFileFactory.FILE_REPLICATION_FACTOR + "=3}")
        .doesNotContainKey("plain");
  }

  @Test
  public void testFileIOWithoutSupportRejectsProperties() {
    InMemoryFileIO io = new InMemoryFileIO();
    assertThatThrownBy(
            () ->
                io.newOutputFile(
                    "memory:/file", Map.of(OutputFileFactory.FILE_REPLICATION_FACTOR, "3")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("memory:/file");
  }

  /** Records which newOutputFile variant was called, delegating to a Hadoop file IO. */
  private class RecordingFileIO extends HadoopFileIO {
    private final Map<String, String> seen;

    RecordingFileIO(Map<String, String> seen) {
      super(conf);
      this.seen = seen;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      seen.put("plain", "true");
      return super.newOutputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path, Map<String, String> properties) {
      seen.put("props", properties.toString());
      return super.newOutputFile(path, properties);
    }
  }
}
