/**
 * Copyright 2022 Dremio
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.support.diagnostics.queriesjson;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.*;
import org.junit.jupiter.api.Test;

public class SummaryTest {

  @Test
  public void testSummary() {
    Query maxAttempts = new Query();
    maxAttempts.setQueryText("max attempts");
    Query maxCommandPool = new Query();
    maxCommandPool.setQueryText("max command pool");
    Query maxMemory = new Query();
    maxMemory.setQueryText("max memory");
    Query maxPending = new Query();
    maxPending.setQueryText("max pending");
    Query maxMetadata = new Query();
    maxMetadata.setQueryText("max metadata");

    Summary summary = new Summary();
    long startTime = 100L;
    long endTime = 99L;
    summary.setFinish(endTime);
    summary.setStart(startTime);
    summary.setMaxAttemptsQuery(maxAttempts);
    summary.setMaxCommandPoolQuery(maxCommandPool);
    summary.setMaxMemoryQuery(maxMemory);
    summary.setMaxPendingQuery(maxPending);
    summary.setMaxMetadataRetrievalQuery(maxMetadata);
    Map<Long, List<SummaryQuery>> map = new HashMap<>();
    List<SummaryQuery> summaryQueries = Collections.singletonList(new SummaryQuery());
    map.put(1L, summaryQueries);
    summary.setBuckets(map);

    assertThat(summary.getFinish()).isEqualTo(endTime);
    assertThat(summary.getStart()).isEqualTo(startTime);
    assertThat(summary.getMaxAttemptsQuery()).isEqualTo(maxAttempts);
    assertThat(summary.getMaxCommandPoolQuery()).isEqualTo(maxCommandPool);
    assertThat(summary.getMaxMemoryQuery()).isEqualTo(maxMemory);
    assertThat(summary.getMaxPendingQuery()).isEqualTo(maxPending);
    assertThat(summary.getMaxMetadataRetrievalQuery()).isEqualTo(maxMetadata);
    assertThat(summary.getBuckets()).isEqualTo(map);
  }

  @Test
  public void testBusiestBucket() {
    Summary summary = new Summary();
    Map<Long, List<SummaryQuery>> map = new HashMap<>();
    long busiestTimestamp = 2L;
    map.put(1L, Arrays.asList(new SummaryQuery(), new SummaryQuery()));
    map.put(
        busiestTimestamp,
        Arrays.asList(new SummaryQuery(), new SummaryQuery(), new SummaryQuery()));
    map.put(3L, Collections.singletonList(new SummaryQuery()));
    summary.setBuckets(map);
    assertThat(summary.getBusiestBucket().getTimestamp()).isEqualTo(busiestTimestamp);
  }

  @Test
  public void testBusiestBucketWithOneQueryOneBucket() {
    Summary summary = new Summary();
    Map<Long, List<SummaryQuery>> map = new HashMap<>();
    long busiestTimestamp = 1L;
    SummaryQuery myQuery = new SummaryQuery();
    myQuery.setQueryText("test");
    myQuery.setStartEpochMillis(1L);
    myQuery.setFinishEpochMillis(100L);
    map.put(busiestTimestamp, Collections.singletonList(myQuery));
    summary.setBuckets(map);
    assertThat(summary.getBusiestBucket().getTimestamp()).isEqualTo(busiestTimestamp);
    assertThat(summary.getBusiestBucket().getQueries().size()).isEqualTo(1);
    assertThat(summary.getBusiestBucket().getQueries().get(0)).isEqualTo(myQuery);
  }
}
