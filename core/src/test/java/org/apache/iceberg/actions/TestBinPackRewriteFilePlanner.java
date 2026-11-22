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
package org.apache.iceberg.actions;

import static org.apache.iceberg.actions.BinPackRewriteFilePlanner.MAX_FILE_SIZE_DEFAULT_RATIO;
import static org.apache.iceberg.actions.RewriteDataFiles.REWRITE_JOB_ORDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestBinPackRewriteFilePlanner {
  private static final Map<String, String> REWRITE_ALL =
      ImmutableMap.of(BinPackRewriteFilePlanner.REWRITE_ALL, "true");

  private static final DataFile FILE_1 = newDataFile("data_bucket=0", 10);
  private static final DataFile FILE_2 = newDataFile("data_bucket=0", 10);
  private static final DataFile FILE_3 = newDataFile("data_bucket=0", 10);
  private static final DataFile FILE_4 = newDataFile("data_bucket=1", 11);
  private static final DataFile FILE_5 = newDataFile("data_bucket=1", 11);
  private static final DataFile FILE_6 = newDataFile("data_bucket=2", 50);

  private static final Map<RewriteJobOrder, List<StructLike>> EXPECTED =
      ImmutableMap.of(
          RewriteJobOrder.FILES_DESC,
          ImmutableList.of(FILE_1.partition(), FILE_4.partition(), FILE_6.partition()),
          RewriteJobOrder.FILES_ASC,
          ImmutableList.of(FILE_6.partition(), FILE_4.partition(), FILE_1.partition()),
          RewriteJobOrder.BYTES_DESC,
          ImmutableList.of(FILE_6.partition(), FILE_1.partition(), FILE_4.partition()),
          RewriteJobOrder.BYTES_ASC,
          ImmutableList.of(FILE_4.partition(), FILE_1.partition(), FILE_6.partition()));

  @TempDir private File tableDir = null;
  private TestTables.TestTable table = null;

  @BeforeEach
  public void setupTable() throws Exception {
    this.table = TestTables.create(tableDir, "test", TestBase.SCHEMA, TestBase.SPEC, 3);
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  void testPartitionedTable() {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(ImmutableMap.of(BinPackRewriteFilePlanner.REWRITE_ALL, "true"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups).hasSize(3);
    assertThat(groups.stream().mapToInt(RewriteGroupBase::inputFileNum).sum()).isEqualTo(6);
  }

  @Test
  void testUnpartitionedTable() {
    table.updateSpec().removeField("data_bucket").commit();
    table.refresh();
    table
        .newAppend()
        .appendFile(newDataFile("", 10))
        .appendFile(newDataFile("", 20))
        .appendFile(newDataFile("", 30))
        .commit();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_INPUT_FILES,
            "1",
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES,
            "30"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(plan.totalGroupCount()).isEqualTo(1);
    assertThat(plan.groups().iterator().next().inputFileNum()).isEqualTo(2);
  }

  @Test
  void testEmptyTable() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();
    assertThat(plan.totalGroupCount()).isZero();
  }

  @ParameterizedTest
  @EnumSource(
      value = RewriteJobOrder.class,
      names = {"FILES_DESC", "FILES_ASC", "BYTES_DESC", "BYTES_ASC"})
  void testJobOrder(RewriteJobOrder order) {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL, "true", REWRITE_JOB_ORDER, order.name()));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups.stream().map(group -> group.info().partition()).collect(Collectors.toList()))
        .isEqualTo(EXPECTED.get(order));
    assertThat(plan.totalGroupCount()).isEqualTo(3);
    EXPECTED.get(order).forEach(s -> assertThat(plan.groupsInPartition(s)).isEqualTo(1));
  }

  @Test
  void testMaxGroupSize() {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true",
            BinPackRewriteFilePlanner.MAX_FILE_GROUP_SIZE_BYTES,
            "10"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(plan.totalGroupCount()).isEqualTo(6);
    assertThat(plan.groupsInPartition(FILE_1.partition())).isEqualTo(3);
    assertThat(plan.groupsInPartition(FILE_4.partition())).isEqualTo(2);
    assertThat(plan.groupsInPartition(FILE_6.partition())).isEqualTo(1);
  }

  @Test
  void testMaxFileGroupInputFiles() {
    addFiles();
    // First, establish baseline without the constraint
    BinPackRewriteFilePlanner baselinePlanner = new BinPackRewriteFilePlanner(table);
    baselinePlanner.init(REWRITE_ALL);
    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> baselinePlan =
        baselinePlanner.plan();
    int baselineGroupCount = baselinePlan.totalGroupCount();
    int baselineGroupsInPartition0 = baselinePlan.groupsInPartition(FILE_1.partition());

    // Now test with max-file-group-input-files set to 2, which limits each group to 2 files
    // Partition 0 has 3 files (FILE_1, FILE_2, FILE_3), so it should be split into 2 groups
    // Partition 1 has 2 files (FILE_4, FILE_5), so it should be 1 group
    // Partition 2 has 1 file (FILE_6), so it should be 1 group
    BinPackRewriteFilePlanner constrainedPlanner = new BinPackRewriteFilePlanner(table);
    constrainedPlanner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true",
            BinPackRewriteFilePlanner.MAX_FILE_GROUP_INPUT_FILES,
            "2"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> constrainedPlan =
        constrainedPlanner.plan();

    // Verify the constraint is honored: should have MORE groups when input files are limited
    assertThat(constrainedPlan.totalGroupCount()).isGreaterThan(baselineGroupCount).isEqualTo(4);
    assertThat(constrainedPlan.groupsInPartition(FILE_1.partition()))
        .isGreaterThan(baselineGroupsInPartition0)
        .isEqualTo(2);
    assertThat(constrainedPlan.groupsInPartition(FILE_4.partition())).isEqualTo(1);
    assertThat(constrainedPlan.groupsInPartition(FILE_6.partition())).isEqualTo(1);
  }

  @Test
  void testFilter() {
    addFiles();
    BinPackRewriteFilePlanner planner =
        new BinPackRewriteFilePlanner(
            table,
            Expressions.or(
                Expressions.equal(Expressions.bucket("data", 16), 0),
                Expressions.equal(Expressions.bucket("data", 16), 2)));
    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(plan.totalGroupCount()).isEqualTo(2);
    assertThat(groups).as("Only partitions for bucket 0 and 2 should be in the plan").hasSize(2);
    assertThat(groups.stream().mapToLong(RewriteFileGroup::inputFileNum).sum())
        .as("Only files matching bucket 0 and 2 should be in the plan")
        .isEqualTo(4);
  }

  @Test
  void testMaxOutputFileSize() {
    addFiles();
    int targetFileSize = 10;
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES,
            String.valueOf(targetFileSize)));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    plan.groups()
        .forEach(
            group ->
                assertThat(group.maxOutputFileSize())
                    .isGreaterThan(targetFileSize)
                    .isLessThan((long) (targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO)));
  }

  @Test
  void testExpectedOutputFiles() {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES,
            "21"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups).hasSize(3);
    for (RewriteFileGroup group : groups) {
      if (FILE_1.partition().equals(group.info().partition())) {
        assertThat(group.expectedOutputFiles()).isEqualTo(2);
      } else if (FILE_4.partition().equals(group.info().partition())) {
        // The result file size is slightly bigger than target, but still below the 10% threshold
        assertThat(group.expectedOutputFiles())
            .as("Should be 1, because the size is below target * 1.1")
            .isEqualTo(1);
      } else if (FILE_6.partition().equals(group.info().partition())) {
        assertThat(group.expectedOutputFiles()).isEqualTo(3);
      } else {
        throw new IllegalStateException("Unexpected partition: " + group.info().partition());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testOutputSpec(boolean useOldSpecId) {
    addFiles();
    int oldSpecId = table.spec().specId();
    table.updateSpec().removeField("data_bucket").commit();
    table.newAppend().appendFile(newDataFile("", 10)).commit();
    table.refresh();
    int newSpecId = table.spec().specId();

    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    if (useOldSpecId) {
      options.put(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(oldSpecId));
    }

    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(options);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    plan.groups()
        .forEach(
            group ->
                assertThat(group.outputSpecId()).isEqualTo(useOldSpecId ? oldSpecId : newSpecId));
  }

  @Test
  void testValidOptions() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MIN_INPUT_FILES,
                BinPackRewriteFilePlanner.REWRITE_ALL,
                BinPackRewriteFilePlanner.MAX_FILE_GROUP_SIZE_BYTES,
                BinPackRewriteFilePlanner.MAX_FILE_GROUP_INPUT_FILES,
                BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD,
                BinPackRewriteFilePlanner.DELETE_RATIO_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER,
                BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE));
  }

  @Test
  void testInvalidOption() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);

    Map<String, String> invalidRewriteJobOrderOptions =
        ImmutableMap.of(RewriteDataFiles.REWRITE_JOB_ORDER, "foo");
    assertThatThrownBy(() -> planner.init(invalidRewriteJobOrderOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");

    Map<String, String> invalidOutputSpecIdOptions =
        ImmutableMap.of(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(1234));
    assertThatThrownBy(() -> planner.init(invalidOutputSpecIdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot use output spec id 1234 because the table does not contain a reference to this spec-id.");

    Map<String, String> invalidDeleteFileThresholdOptions =
        ImmutableMap.of(BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(invalidDeleteFileThresholdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");

    Map<String, String> negativeDeleteRatioThresholdOptions =
        ImmutableMap.of(BinPackRewriteFilePlanner.DELETE_RATIO_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(negativeDeleteRatioThresholdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'delete-ratio-threshold' is set to -1.0 but must be > 0");

    Map<String, String> invalidDeleteRatioThresholdOptions =
        ImmutableMap.of(BinPackRewriteFilePlanner.DELETE_RATIO_THRESHOLD, "127");
    assertThatThrownBy(() -> planner.init(invalidDeleteRatioThresholdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'delete-ratio-threshold' is set to 127.0 but must be <= 1");
  }

  @Test
  void testSizeFiltering() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    FileScanTask tooSmallTask = new MockFileScanTask(100L);
    FileScanTask optimal = new MockFileScanTask(450);
    FileScanTask tooBigTask = new MockFileScanTask(1000L);
    List<FileScanTask> tasks = ImmutableList.of(tooSmallTask, optimal, tooBigTask);

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "250",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "500",
            BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, "750");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 2 files").hasSize(2);
    assertThat(group).containsExactlyInAnyOrder(tooSmallTask, tooBigTask);
  }

  @Test
  void testGroupWithEnoughFiles() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_INPUT_FILES, "3",
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "150",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "1000",
            BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, "5000");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 4 files").hasSize(4);
  }

  @Test
  void testGroupWithEnoughData() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L), new MockFileScanTask(100L), new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "200",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "250",
            BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, "500");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 3 files").hasSize(3);
  }

  @Test
  void testFileSizeAboveTarget() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    List<FileScanTask> tasks = ImmutableList.of(new MockFileScanTask(2000L));

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "200",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "250",
            BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, "500");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite big file").hasSize(1);
  }

  @Test
  void testDeleteFileThreshold() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    FileScanTask tooManyDeletesTask = MockFileScanTask.mockTaskWithDeletes(1000L, 3);
    FileScanTask optimalTask = MockFileScanTask.mockTaskWithDeletes(1001L, 1);
    List<FileScanTask> tasks = ImmutableList.of(tooManyDeletesTask, optimalTask);

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "0",
            BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, "2");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 1 file").hasSize(1);
    assertThat(group).containsExactly(tooManyDeletesTask);
  }

  @Test
  void testDeleteRatioThreshold() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    FileScanTask tooManyDeletesTask =
        MockFileScanTask.mockTaskWithFileScopedDeleteRecords(1000L, 100, 1, 20);
    FileScanTask optimalTask =
        MockFileScanTask.mockTaskWithFileScopedDeleteRecords(1001L, 100, 1, 19);
    List<FileScanTask> tasks = ImmutableList.of(tooManyDeletesTask, optimalTask);

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "0",
            BinPackRewriteFilePlanner.DELETE_RATIO_THRESHOLD, "0.2");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 1 file").hasSize(1);
    assertThat(group).containsExactly(tooManyDeletesTask);
  }

  @Test
  public void testInvalidMaxFilesRewriteParam() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    Map<String, String> invalidMaxFilesToDeleteOption =
        ImmutableMap.of(BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE, "0");
    assertThatThrownBy(() -> planner.init(invalidMaxFilesToDeleteOption))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set max-files-to-rewrite to 0, the value must be positive integer.");

    Map<String, String> negativeMaxFilesToDeleteOption =
        ImmutableMap.of(BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE, "-2");
    assertThatThrownBy(() -> planner.init(negativeMaxFilesToDeleteOption))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set max-files-to-rewrite to -2, the value must be positive integer.");
  }

  @Test
  public void textMaxFilesRewriteToOnlyTruncateNeededPartitions() {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE,
            "4",
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true");
    planner.init(options);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();
    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());

    List<Integer> fileScanTasksListSize =
        groups.stream()
            .map(RewriteGroupBase::fileScanTasks)
            .map(List::size)
            .sorted()
            .collect(Collectors.toList());
    assertThat(fileScanTasksListSize)
        .isIn(Arrays.asList(Arrays.asList(1, 3), Arrays.asList(1, 1, 2), Arrays.asList(2, 2)));
    assertThat(fileScanTasksListSize.stream().reduce(0, Integer::sum)).isEqualTo(4);
  }

  @Test
  public void testRewriteMaxFilesOption() {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE,
            "5",
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true");
    planner.init(options);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();
    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());

    Integer fileScanTasks =
        groups.stream()
            .map(RewriteGroupBase::fileScanTasks)
            .map(List::size)
            .reduce(0, Integer::sum);
    assertThat(fileScanTasks).isEqualTo(5);
  }

  @Test
  public void testMaxFilesToRewriteUsesCorrectInputSizeForExpectedOutputFiles() {
    // Create files with known sizes where truncation matters for expectedOutputFiles
    DataFile largeFile1 = newDataFile("data_bucket=0", 200);
    DataFile largeFile2 = newDataFile("data_bucket=0", 200);
    DataFile largeFile3 = newDataFile("data_bucket=0", 200);
    table.newAppend().appendFile(largeFile1).appendFile(largeFile2).appendFile(largeFile3).commit();

    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    // target=250 means:
    //   Full group (600 bytes): expectedOutputFiles = ceil(600/250) = 3
    //   Truncated to 1 file (200 bytes): expectedOutputFiles = ceil(200/250) = 1
    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE, "1",
            BinPackRewriteFilePlanner.REWRITE_ALL, "true",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "250");
    planner.init(options);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();
    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());

    assertThat(groups).hasSize(1);
    RewriteFileGroup group = groups.get(0);
    // Only 1 file should be in the group due to max-files-to-rewrite=1
    assertThat(group.inputFileNum()).isEqualTo(1);
    // expectedOutputFiles should be based on the truncated input (200 bytes), not full group (600)
    // ceil(200/250) = 1, NOT ceil(600/250) = 3
    assertThat(group.expectedOutputFiles())
        .as(
            "expectedOutputFiles should be computed from actual files being rewritten (200 bytes),"
                + " not the full group (600 bytes)")
        .isEqualTo(1);
  }

  @Test
  public void testRewriteMaxFilesRewriteGreaterThanTotalFiles() {
    addFiles();
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE,
            "500",
            BinPackRewriteFilePlanner.REWRITE_ALL,
            "true");
    planner.init(options);
    int numFiles = Iterables.size(table.newScan().planFiles());

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();
    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());

    Integer fileScanTasks =
        groups.stream()
            .map(RewriteGroupBase::fileScanTasks)
            .map(List::size)
            .reduce(0, Integer::sum);
    assertThat(fileScanTasks).isLessThanOrEqualTo(numFiles).isLessThanOrEqualTo(500);
  }

  @Test
  void testJobOrderFilesMinSequenceNumberAsc() {
    addFilesInSeparateCommits(
        newDataFile("data_bucket=2", 50),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=0", 10),
        newDataFile("data_bucket=0", 10));

    List<RewriteFileGroup> groups = planGroups(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, null);

    // minimum file sequence numbers of the groups: bucket=2 -> 1, bucket=1 -> 2, bucket=0 -> 4
    assertThat(groups.stream().map(RewriteFileGroup::minFileSequenceNumber))
        .containsExactly(1L, 2L, 4L);
    assertThat(groups.stream().map(group -> group.info().partition().get(0, Integer.class)))
        .containsExactly(2, 1, 0);
  }

  @Test
  void testJobOrderFilesMinSequenceNumberDesc() {
    addFilesInSeparateCommits(
        newDataFile("data_bucket=2", 50),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=0", 10),
        newDataFile("data_bucket=0", 10));

    List<RewriteFileGroup> groups =
        planGroups(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC, null);

    assertThat(groups.stream().map(RewriteFileGroup::minFileSequenceNumber))
        .containsExactly(4L, 2L, 1L);
    assertThat(groups.stream().map(group -> group.info().partition().get(0, Integer.class)))
        .containsExactly(0, 1, 2);
  }

  @Test
  void testMaxTotalFilesSizeBytesSelectsOldestFilesFirst() {
    DataFile d = newDataFile("data_bucket=2", 50);
    DataFile b = newDataFile("data_bucket=1", 11);
    DataFile c = newDataFile("data_bucket=1", 11);
    DataFile a = newDataFile("data_bucket=0", 10);
    DataFile e = newDataFile("data_bucket=0", 10);
    addFilesInSeparateCommits(d, b, c, a, e);

    // all files within the budget
    assertThat(
            selectedFiles(
                RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, String.valueOf(50 + 11 + 11 + 20)))
        .containsExactlyInAnyOrder(
            d.location(), b.location(), c.location(), a.location(), e.location());

    // the budget is exactly the size of the 3 oldest files: they are all included
    assertThat(
            selectedFiles(
                RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, String.valueOf(50 + 11 + 11)))
        .containsExactlyInAnyOrder(d.location(), b.location(), c.location());

    // one byte less: the file crossing the budget is excluded
    assertThat(
            selectedFiles(
                RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, String.valueOf(50 + 11 + 10)))
        .containsExactlyInAnyOrder(d.location(), b.location());

    // exactly the first file
    assertThat(selectedFiles(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, "50"))
        .containsExactly(d.location());

    // the very first file does not fit: nothing is selected
    assertThat(selectedFiles(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, "49")).isEmpty();
  }

  @Test
  void testMaxTotalFilesSizeBytesSelectsNewestFilesFirst() {
    DataFile d = newDataFile("data_bucket=2", 50);
    DataFile b = newDataFile("data_bucket=1", 11);
    DataFile c = newDataFile("data_bucket=1", 11);
    DataFile a = newDataFile("data_bucket=0", 10);
    DataFile e = newDataFile("data_bucket=0", 12);
    addFilesInSeparateCommits(d, b, c, a, e);

    // newest first: e (12), a (10), c (11), b (11), d (50)
    assertThat(selectedFiles(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC, "33"))
        .containsExactlyInAnyOrder(e.location(), a.location(), c.location());
    assertThat(selectedFiles(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC, "32"))
        .containsExactlyInAnyOrder(e.location(), a.location());
  }

  @Test
  void testMaxTotalFilesSizeBytesStopsAtFirstFileThatDoesNotFit() {
    DataFile x = newDataFile("data_bucket=0", 10);
    DataFile y = newDataFile("data_bucket=0", 100);
    DataFile z = newDataFile("data_bucket=0", 10);
    addFilesInSeparateCommits(x, y, z);

    // z would fit into the remaining budget, but selection stops at y
    assertThat(selectedFiles(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, "30"))
        .containsExactly(x.location());
  }

  @Test
  void testMaxTotalFilesSizeBytesWithoutJobOrderUsesScanOrder() {
    // a single commit (one manifest) makes the order of the scan tasks deterministic
    addFilesInOneCommit(
        newDataFile("data_bucket=2", 50),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=0", 10),
        newDataFile("data_bucket=0", 10));

    List<FileScanTask> scanOrder = Lists.newArrayList(table.newScan().planFiles());
    long budget = scanOrder.get(0).length() + scanOrder.get(1).length() + scanOrder.get(2).length();

    assertThat(selectedFiles(RewriteJobOrder.NONE, String.valueOf(budget)))
        .containsExactlyInAnyOrder(
            scanOrder.get(0).file().location(),
            scanOrder.get(1).file().location(),
            scanOrder.get(2).file().location());
  }

  @Test
  void testMaxTotalFilesSizeBytesAppliesBeforeFilteringFiles() {
    DataFile big = newDataFile("data_bucket=0", 100);
    DataFile small1 = newDataFile("data_bucket=0", 10);
    DataFile small2 = newDataFile("data_bucket=0", 10);
    addFilesInSeparateCommits(big, small1, small2);

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "100",
            BinPackRewriteFilePlanner.MIN_INPUT_FILES, "2");

    BinPackRewriteFilePlanner unbudgeted = new BinPackRewriteFilePlanner(table);
    unbudgeted.init(options);
    assertThat(unbudgeted.plan().totalGroupCount())
        .as("The two small files are rewritten when there is no budget")
        .isEqualTo(1);

    // "big" is not a rewrite candidate, but it still consumes the budget as it is selected first
    Map<String, String> budgeted = Maps.newHashMap(options);
    budgeted.put(REWRITE_JOB_ORDER, RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC.orderName());
    budgeted.put(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, "105");
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(budgeted);
    assertThat(planner.plan().totalGroupCount()).isZero();
  }

  @Test
  void testMaxTotalFilesSizeBytesUnsetOrNullMeansNoLimit() {
    addFiles();
    List<String> all = selectedFiles(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, null);
    assertThat(all).hasSize(6);

    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    options.put(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, null);
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(options);
    assertThat(planner.plan().totalGroupCount()).isEqualTo(3);

    assertThat(
            selectedFiles(
                RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC, String.valueOf(Long.MAX_VALUE)))
        .containsExactlyInAnyOrderElementsOf(all);
    assertThat(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES_DEFAULT).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void testMaxTotalFilesSizeBytesZeroAndNegativeSelectNothing() {
    addFiles();
    assertThat(selectedFiles(RewriteJobOrder.NONE, "0")).isEmpty();
    assertThat(selectedFiles(RewriteJobOrder.NONE, "-1")).isEmpty();
  }

  @Test
  void testMaxTotalFilesSizeBytesIsNotValidated() {
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(ImmutableMap.of(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, "-5"));
    assertThatThrownBy(
            () -> planner.init(ImmutableMap.of(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, "abc")))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  void testMaxTotalFilesSizeBytesCountsDeleteFiles() throws Exception {
    // position delete files are not allowed in v3 tables
    TestTables.TestTable v2Table =
        TestTables.create(new File(tableDir, "v2"), "test-v2", TestBase.SCHEMA, TestBase.SPEC, 2);
    DataFile data = newDataFile("data_bucket=0", 10);
    DeleteFile deletes =
        FileMetadata.deleteFileBuilder(TestBase.SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/deletes-" + UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(100)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    v2Table.newAppend().appendFile(data).commit();
    v2Table.newRowDelta().addDeletes(deletes).commit();

    FileScanTask task = Iterables.getOnlyElement(v2Table.newScan().planFiles());
    assertThat(task.length()).isEqualTo(10);
    assertThat(task.sizeBytes()).isEqualTo(110);

    // the size of the delete file counts towards the budget, 110 > 50
    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    options.put(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, "50");
    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(v2Table);
    planner.init(options);
    assertThat(planner.plan().totalGroupCount()).isZero();

    options.put(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, "110");
    planner = new BinPackRewriteFilePlanner(v2Table);
    planner.init(options);
    assertThat(planner.plan().totalGroupCount()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(
      value = RewriteJobOrder.class,
      names = {"NONE", "FILES_DESC", "FILES_ASC", "BYTES_DESC", "BYTES_ASC"})
  void testPlanWithoutBudgetMatchesPlanningScanTasksDirectly(RewriteJobOrder order) {
    // a single commit (one manifest) makes the order of the scan tasks deterministic
    addFilesInOneCommit(
        newDataFile("data_bucket=2", 50),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=1", 11),
        newDataFile("data_bucket=0", 10),
        newDataFile("data_bucket=0", 10),
        newDataFile("data_bucket=0", 10),
        newDataFile("data_bucket=3", 10));

    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    options.put(REWRITE_JOB_ORDER, order.orderName());
    options.put(BinPackRewriteFilePlanner.MAX_FILE_GROUP_SIZE_BYTES, "25");

    // Reference: group the scan tasks of the table by partition in scan order, bin-pack each
    // partition, and order the groups with a test local comparator. This is what the planner did
    // before max-total-files-size-bytes and the sequence number orders were added.
    BinPackRewriteFilePlanner referencePlanner = new BinPackRewriteFilePlanner(table);
    referencePlanner.init(options);
    StructLikeMap<List<FileScanTask>> byPartition =
        StructLikeMap.create(table.spec().partitionType());
    for (FileScanTask task : table.newScan().planFiles()) {
      byPartition
          .computeIfAbsent(task.file().partition(), unused -> Lists.newArrayList())
          .add(task);
    }

    List<List<String>> expected = Lists.newArrayList();
    byPartition
        .values()
        .forEach(
            tasks ->
                referencePlanner
                    .planFileGroups(tasks)
                    .forEach(
                        group ->
                            expected.add(
                                group.stream()
                                    .map(task -> task.file().location())
                                    .collect(Collectors.toList()))));
    Map<String, FileScanTask> tasksByPath = Maps.newHashMap();
    byPartition
        .values()
        .forEach(tasks -> tasks.forEach(t -> tasksByPath.put(t.file().location(), t)));
    ToLongFunction<List<String>> bytes =
        group -> group.stream().mapToLong(path -> tasksByPath.get(path).length()).sum();
    switch (order) {
      case BYTES_ASC:
        expected.sort(Comparator.comparingLong(bytes));
        break;
      case BYTES_DESC:
        expected.sort(Comparator.comparingLong(bytes).reversed());
        break;
      case FILES_ASC:
        expected.sort(Comparator.comparingInt(List::size));
        break;
      case FILES_DESC:
        expected.sort(Comparator.comparingInt(List<String>::size).reversed());
        break;
      default:
        break;
    }

    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(options);
    List<List<String>> actual =
        Lists.newArrayList(planner.plan().groups().iterator()).stream()
            .map(
                group ->
                    group.fileScanTasks().stream()
                        .map(task -> task.file().location())
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());

    // groups of different partitions with the same key (bytes or files) are ordered by the
    // iteration order of the partitions, which is not specified, so ties are not compared
    assertThat(actual).isNotEmpty().containsExactlyInAnyOrderElementsOf(expected);
    switch (order) {
      case BYTES_ASC:
      case BYTES_DESC:
        assertThat(actual.stream().mapToLong(bytes).boxed().collect(Collectors.toList()))
            .isEqualTo(expected.stream().mapToLong(bytes).boxed().collect(Collectors.toList()));
        break;
      case FILES_ASC:
      case FILES_DESC:
        assertThat(actual.stream().map(List::size).collect(Collectors.toList()))
            .isEqualTo(expected.stream().map(List::size).collect(Collectors.toList()));
        break;
      default:
        break;
    }
  }

  private void addFilesInOneCommit(DataFile... files) {
    AppendFiles append = table.newAppend();
    for (DataFile file : files) {
      append.appendFile(file);
    }

    append.commit();
  }

  private void addFilesInSeparateCommits(DataFile... files) {
    for (DataFile file : files) {
      table.newAppend().appendFile(file).commit();
    }
  }

  private List<RewriteFileGroup> planGroups(RewriteJobOrder order, String maxTotalFilesSizeBytes) {
    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    options.put(REWRITE_JOB_ORDER, order.orderName());
    if (maxTotalFilesSizeBytes != null) {
      options.put(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, maxTotalFilesSizeBytes);
    }

    BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
    planner.init(options);
    return Lists.newArrayList(planner.plan().groups().iterator());
  }

  private List<String> selectedFiles(RewriteJobOrder order, String maxTotalFilesSizeBytes) {
    return planGroups(order, maxTotalFilesSizeBytes).stream()
        .flatMap(group -> group.fileScanTasks().stream())
        .map(task -> task.file().location())
        .collect(Collectors.toList());
  }

  private void addFiles() {
    table
        .newAppend()
        .appendFile(FILE_1)
        .appendFile(FILE_2)
        .appendFile(FILE_3)
        .appendFile(FILE_4)
        .appendFile(FILE_5)
        .appendFile(FILE_6)
        .commit();
  }

  private static DataFile newDataFile(String partitionPath, long fileSize) {
    return DataFiles.builder(TestBase.SPEC)
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(fileSize)
        .withPartitionPath(partitionPath)
        .withRecordCount(1)
        .build();
  }
}
