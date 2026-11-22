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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Local file system that records the replication factor passed to each stream creation. */
public class ReplicationCapturingFileSystem extends RawLocalFileSystem {
  private static final Map<String, Short> CAPTURED = Maps.newConcurrentMap();

  /** Returns a configuration that routes file: paths to this file system, bypassing the cache. */
  public static Configuration newConf() {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", ReplicationCapturingFileSystem.class, FileSystem.class);
    conf.setBoolean("fs.file.impl.disable.cache", true);
    return conf;
  }

  /** Returns the replication factor used to create the file with the given name, or null. */
  public static Short capturedReplication(String fileName) {
    return CAPTURED.get(fileName);
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
    return super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
  }
}
