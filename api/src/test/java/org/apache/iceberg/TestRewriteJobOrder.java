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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.actions.RewriteDataFiles;
import org.junit.jupiter.api.Test;

public class TestRewriteJobOrder {

  @Test
  public void testFromNameForAllOrders() {
    for (RewriteJobOrder order : RewriteJobOrder.values()) {
      assertThat(RewriteJobOrder.fromName(order.orderName())).isEqualTo(order);
    }
  }

  @Test
  public void testFromNameWithMultipleHyphens() {
    assertThat(RewriteJobOrder.fromName("files-min-sequence-number-asc"))
        .isEqualTo(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC);
    assertThat(RewriteJobOrder.fromName("files-min-sequence-number-desc"))
        .isEqualTo(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC);
  }

  @Test
  public void testOrderNames() {
    assertThat(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC.orderName())
        .isEqualTo("files-min-sequence-number-asc");
    assertThat(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC.orderName())
        .isEqualTo("files-min-sequence-number-desc");
  }

  @Test
  public void testFromNameIsCaseInsensitive() {
    assertThat(RewriteJobOrder.fromName("FILES-MIN-SEQUENCE-NUMBER-ASC"))
        .isEqualTo(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC);
    assertThat(RewriteJobOrder.fromName("Bytes-Asc")).isEqualTo(RewriteJobOrder.BYTES_ASC);
  }

  @Test
  public void testFromNameInvalid() {
    assertThatThrownBy(() -> RewriteJobOrder.fromName("foo"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");
    assertThatThrownBy(() -> RewriteJobOrder.fromName(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: null");
  }

  @Test
  public void testOptionDefaults() {
    assertThat(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES).isEqualTo("max-total-files-size-bytes");
    assertThat(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES_DEFAULT).isEqualTo(Long.MAX_VALUE);
  }
}
