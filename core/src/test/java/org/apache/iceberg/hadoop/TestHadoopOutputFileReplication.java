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
package org.apache.iceberg.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHadoopOutputFileReplication {

  @TempDir private File tempDir;
  private Configuration conf;

  @BeforeEach
  public void before() {
    this.conf = ReplicationCapturingFileSystem.newConf();
  }

  private Path path(String name) {
    return new Path(new File(tempDir, name).toURI());
  }

  private short fsDefaultReplication(Path path) throws IOException {
    return path.getFileSystem(conf).getDefaultReplication(path);
  }

  private static void write(PositionOutputStream stream) throws IOException {
    try (PositionOutputStream out = stream) {
      out.write("data".getBytes());
    }
  }

  @Test
  public void testCreateWithReplication() throws IOException {
    Path path = path("create-replication.txt");
    OutputFile file = HadoopOutputFile.fromPath(path, conf, (short) 5);

    write(file.create());

    assertThat(ReplicationCapturingFileSystem.capturedReplication("create-replication.txt"))
        .isEqualTo((short) 5);
  }

  @Test
  public void testCreateOrOverwriteWithReplication() throws IOException {
    Path path = path("overwrite-replication.txt");
    OutputFile file = HadoopOutputFile.fromPath(path, conf, (short) 4);

    write(file.createOrOverwrite());
    assertThat(ReplicationCapturingFileSystem.capturedReplication("overwrite-replication.txt"))
        .isEqualTo((short) 4);

    write(file.createOrOverwrite());
    assertThat(file.toInputFile().getLength()).isEqualTo(4L);
  }

  @Test
  public void testFileIOWithReplicationProperty() throws IOException {
    HadoopFileIO io = new HadoopFileIO(conf);
    String location = path("fileio-replication.txt").toString();

    OutputFile file =
        io.newOutputFile(location, ImmutableMap.of(OutputFileFactory.FILE_REPLICATION_FACTOR, "2"));
    write(file.create());

    assertThat(ReplicationCapturingFileSystem.capturedReplication("fileio-replication.txt"))
        .isEqualTo((short) 2);
  }

  @Test
  public void testDefaultReplicationWhenNotConfigured() throws IOException {
    Path path = path("default-replication.txt");
    HadoopFileIO io = new HadoopFileIO(conf);

    write(io.newOutputFile(path.toString()).create());

    assertThat(ReplicationCapturingFileSystem.capturedReplication("default-replication.txt"))
        .isEqualTo(fsDefaultReplication(path));
  }

  @Test
  public void testDefaultReplicationWhenPropertyMissing() throws IOException {
    Path path = path("missing-property.txt");
    HadoopFileIO io = new HadoopFileIO(conf);

    write(io.newOutputFile(path.toString(), ImmutableMap.of()).create());

    assertThat(ReplicationCapturingFileSystem.capturedReplication("missing-property.txt"))
        .isEqualTo(fsDefaultReplication(path));
  }

  @Test
  public void testDefaultReplicationWhenPropertyInvalid() throws IOException {
    Path path = path("invalid-property.txt");
    OutputFile file =
        HadoopOutputFile.fromPath(
            path, conf, ImmutableMap.of(OutputFileFactory.FILE_REPLICATION_FACTOR, "invalid"));

    write(file.create());

    assertThat(ReplicationCapturingFileSystem.capturedReplication("invalid-property.txt"))
        .isEqualTo(fsDefaultReplication(path));
  }

  @Test
  public void testNonPositiveReplicationUsesDefault() throws IOException {
    Path zero = path("zero-replication.txt");
    Path negative = path("negative-replication.txt");

    write(HadoopOutputFile.fromPath(zero, conf, (short) 0).create());
    write(HadoopOutputFile.fromPath(negative, conf, (short) -1).create());

    assertThat(ReplicationCapturingFileSystem.capturedReplication("zero-replication.txt"))
        .isEqualTo(fsDefaultReplication(zero));
    assertThat(ReplicationCapturingFileSystem.capturedReplication("negative-replication.txt"))
        .isEqualTo(fsDefaultReplication(negative));
  }
}
