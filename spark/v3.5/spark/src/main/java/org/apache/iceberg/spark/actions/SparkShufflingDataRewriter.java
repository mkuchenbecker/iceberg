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
package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkFunctionCatalog;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalesce;
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalescer;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.execution.datasources.v2.DistributionAndOrderingUtils$;
import scala.Option;

abstract class SparkShufflingDataRewriter extends SparkSizeBasedDataRewriter {

  /**
   * The number of shuffle partitions and consequently the number of output files created by the
   * Spark sort is based on the size of the input data files used in this file rewriter. Due to
   * compression, the disk file sizes may not accurately represent the size of files in the output.
   * This parameter lets the user adjust the file size used for estimating actual output data size.
   * A factor greater than 1.0 would generate more files than we would expect based on the on-disk
   * file size. A value less than 1.0 would create fewer files than we would expect based on the
   * on-disk size.
   */
  public static final String COMPRESSION_FACTOR = "compression-factor";

  public static final double COMPRESSION_FACTOR_DEFAULT = 1.0;

  /**
   * The number of shuffle partitions to use for each output file. By default, this file rewriter
   * assumes each shuffle partition would become a separate output file. Attempting to generate
   * large output files of 512 MB or higher may strain the memory resources of the cluster as such
   * rewrites would require lots of Spark memory. This parameter can be used to further divide up
   * the data which will end up in a single file. For example, if the target file size is 2 GB, but
   * the cluster can only handle shuffles of 512 MB, this parameter could be set to 4. Iceberg will
   * use a custom coalesce operation to stitch these sorted partitions back together into a single
   * sorted file.
   *
   * <p>Note using this parameter requires enabling Iceberg Spark session extensions.
   */
  public static final String SHUFFLE_PARTITIONS_PER_FILE = "shuffle-partitions-per-file";

  public static final int SHUFFLE_PARTITIONS_PER_FILE_DEFAULT = 1;

  private double compressionFactor;
  private int numShufflePartitionsPerFile;
  private Integer outputSortOrderId;

  protected SparkShufflingDataRewriter(SparkSession spark, Table table) {
    super(spark, table);
  }

  protected abstract org.apache.iceberg.SortOrder sortOrder();

  /**
   * The sort order, bound to the table schema, that describes the layout this rewriter produces.
   * Recorded on the output files as their sort order id when {@link
   * RewriteDataFiles#OUTPUT_SORT_ORDER_ID} is set. Defaults to {@link #sortOrder()}.
   */
  protected org.apache.iceberg.SortOrder layoutSortOrder() {
    return sortOrder();
  }

  protected abstract Dataset<Row> sortedDF(
      Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc);

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COMPRESSION_FACTOR)
        .add(SHUFFLE_PARTITIONS_PER_FILE)
        .add(RewriteDataFiles.OUTPUT_SORT_ORDER_ID)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.compressionFactor = compressionFactor(options);
    this.numShufflePartitionsPerFile = numShufflePartitionsPerFile(options);
    this.outputSortOrderId =
        PropertyUtil.propertyAsNullableInt(options, RewriteDataFiles.OUTPUT_SORT_ORDER_ID);
    Preconditions.checkArgument(
        outputSortOrderId == null || outputSortOrderId != 0,
        "Cannot set %s to 0, it is reserved for unsorted files",
        RewriteDataFiles.OUTPUT_SORT_ORDER_ID);
  }

  /**
   * The layout sort order rebuilt under the configured output sort order id, or null when no id was
   * configured.
   */
  protected org.apache.iceberg.SortOrder outputSortOrder() {
    if (outputSortOrderId == null) {
      return null;
    }

    org.apache.iceberg.SortOrder layout = layoutSortOrder();
    org.apache.iceberg.SortOrder.Builder builder =
        org.apache.iceberg.SortOrder.builderFor(table().schema()).withOrderId(outputSortOrderId);
    for (SortField field : layout.fields()) {
      String column = table().schema().findColumnName(field.sourceId());
      Preconditions.checkArgument(
          column != null,
          "Cannot record sort order id %s: field %s of the layout is not in the table schema",
          outputSortOrderId,
          field.sourceId());
      Term term =
          field.transform().isIdentity()
              ? Expressions.ref(column)
              : Expressions.transform(column, field.transform());
      builder.sortBy(term, field.direction(), field.nullOrder());
    }

    return builder.build();
  }

  @Override
  public void doRewrite(String groupId, List<FileScanTask> group) {
    Dataset<Row> scanDF =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .load(groupId);

    Dataset<Row> sortedDF = sortedDF(scanDF, sortFunction(group));

    DataFrameWriter<Row> writer = withOutputFileMetadata(sortedDF.write());
    org.apache.iceberg.SortOrder outputSortOrder = outputSortOrder();
    if (outputSortOrder != null) {
      writer =
          writer.option(
              SparkWriteOptions.OUTPUT_SORT_ORDER, SortOrderParser.toJson(outputSortOrder));
    }

    writer
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .mode("append")
        .save(groupId);
  }

  private Function<Dataset<Row>, Dataset<Row>> sortFunction(List<FileScanTask> group) {
    SortOrder[] ordering = Spark3Util.toOrdering(outputSortOrder(group));
    int numShufflePartitions = numShufflePartitions(group);
    return (df) -> transformPlan(df, plan -> sortPlan(plan, ordering, numShufflePartitions));
  }

  private LogicalPlan sortPlan(LogicalPlan plan, SortOrder[] ordering, int numShufflePartitions) {
    SparkFunctionCatalog catalog = SparkFunctionCatalog.get();
    OrderedWrite write = new OrderedWrite(ordering, numShufflePartitions);
    LogicalPlan sortPlan =
        DistributionAndOrderingUtils$.MODULE$.prepareQuery(write, plan, Option.apply(catalog));

    if (numShufflePartitionsPerFile == 1) {
      return sortPlan;
    } else {
      OrderAwareCoalescer coalescer = new OrderAwareCoalescer(numShufflePartitionsPerFile);
      int numOutputPartitions = numShufflePartitions / numShufflePartitionsPerFile;
      return new OrderAwareCoalesce(numOutputPartitions, coalescer, sortPlan);
    }
  }

  private Dataset<Row> transformPlan(Dataset<Row> df, Function<LogicalPlan, LogicalPlan> func) {
    return new Dataset<>(spark(), func.apply(df.logicalPlan()), df.encoder());
  }

  private org.apache.iceberg.SortOrder outputSortOrder(List<FileScanTask> group) {
    boolean includePartitionColumns = !group.get(0).spec().equals(table().spec());
    if (includePartitionColumns) {
      // build in the requirement for partition sorting into our sort order
      // as the original spec for this group does not match the output spec
      return SortOrderUtil.buildSortOrder(table(), sortOrder());
    } else {
      return sortOrder();
    }
  }

  private int numShufflePartitions(List<FileScanTask> group) {
    int numOutputFiles = (int) numOutputFiles((long) (inputSize(group) * compressionFactor));
    return Math.max(1, numOutputFiles * numShufflePartitionsPerFile);
  }

  private double compressionFactor(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(options, COMPRESSION_FACTOR, COMPRESSION_FACTOR_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", COMPRESSION_FACTOR, value);
    return value;
  }

  private int numShufflePartitionsPerFile(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(
            options, SHUFFLE_PARTITIONS_PER_FILE, SHUFFLE_PARTITIONS_PER_FILE_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", SHUFFLE_PARTITIONS_PER_FILE, value);
    Preconditions.checkArgument(
        value == 1 || Spark3Util.extensionsEnabled(spark()),
        "Using '%s' requires enabling Iceberg Spark session extensions",
        SHUFFLE_PARTITIONS_PER_FILE);
    return value;
  }

  private static class OrderedWrite implements RequiresDistributionAndOrdering {
    private final OrderedDistribution distribution;
    private final SortOrder[] ordering;
    private final int numShufflePartitions;

    OrderedWrite(SortOrder[] ordering, int numShufflePartitions) {
      this.distribution = Distributions.ordered(ordering);
      this.ordering = ordering;
      this.numShufflePartitions = numShufflePartitions;
    }

    @Override
    public Distribution requiredDistribution() {
      return distribution;
    }

    @Override
    public boolean distributionStrictlyRequired() {
      return true;
    }

    @Override
    public int requiredNumPartitions() {
      return numShufflePartitions;
    }

    @Override
    public SortOrder[] requiredOrdering() {
      return ordering;
    }
  }
}
