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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class SummaryAggregatorTest {
  @Test
  public void testFindMaxValueIsAlwaysZero() {
    long earliestStartTime = 100L;
    long finalFinish = 10000L;
    Query maxMemoryQuery = new Query();
    maxMemoryQuery.setStart(earliestStartTime);
    maxMemoryQuery.setFinish(finalFinish);

    Query maxPendingQuery = new Query();
    maxPendingQuery.setStart(earliestStartTime);

    Query maxMetadataQuery = new Query();
    maxMetadataQuery.setStart(earliestStartTime);

    Query maxAttemptsQuery = new Query();
    maxAttemptsQuery.setStart(earliestStartTime);

    Query maxCommandPoolQuery = new Query();
    maxCommandPoolQuery.setStart(earliestStartTime);

    Query alsoZero = new Query();
    alsoZero.setStart(earliestStartTime);

    Stream<Query> queries =
        Stream.of(
            maxMemoryQuery,
            maxPendingQuery,
            maxAttemptsQuery,
            maxCommandPoolQuery,
            maxMetadataQuery,
            alsoZero);
    SummaryAggregator summaryAggregator = new SummaryAggregator(queries);
    Summary summary = summaryAggregator.generateSummary();
    assertThat(summary.getMaxCommandPoolQuery()).isNull();
    assertThat(summary.getMaxAttemptsQuery()).isNull();
    assertThat(summary.getMaxPendingQuery()).isNull();
    assertThat(summary.getMaxMemoryQuery()).isNull();
    assertThat(summary.getMaxMetadataRetrievalQuery()).isNull();
  }

  @Test
  public void testFindMaxValues() {
    long earliestStartTime = 100L;
    long finalFinish = 10000L;
    Query maxMemoryQuery = new Query();
    maxMemoryQuery.setStart(earliestStartTime);
    maxMemoryQuery.setFinish(finalFinish);
    maxMemoryQuery.setMemoryAllocated(1000L);

    Query maxPendingQuery = new Query();
    maxPendingQuery.setStart(earliestStartTime);
    maxPendingQuery.setPendingTime(1000L);

    Query maxMetadataQuery = new Query();
    maxMetadataQuery.setStart(earliestStartTime);
    maxMetadataQuery.setMetadataRetrieval(1000L);

    Query maxAttemptsQuery = new Query();
    maxAttemptsQuery.setStart(earliestStartTime);
    maxAttemptsQuery.setAttemptCount(1000L);

    Query maxCommandPoolQuery = new Query();
    maxCommandPoolQuery.setStart(earliestStartTime);
    maxCommandPoolQuery.setPoolWaitTime(1000L);

    Query lessThanMax = new Query();
    lessThanMax.setStart(earliestStartTime);
    lessThanMax.setMetadataRetrieval(1L);
    lessThanMax.setPoolWaitTime(1L);
    lessThanMax.setAttemptCount(1L);
    lessThanMax.setMemoryAllocated(1L);
    lessThanMax.setPendingTime(1L);

    Stream<Query> queries =
        Stream.of(
            maxMemoryQuery,
            maxPendingQuery,
            maxAttemptsQuery,
            maxCommandPoolQuery,
            maxMetadataQuery,
            lessThanMax);
    SummaryAggregator summaryAggregator = new SummaryAggregator(queries);
    Summary summary = summaryAggregator.generateSummary();

    assertThat(summary.getStart()).isEqualTo(earliestStartTime);
    assertThat(summary.getFinish()).isEqualTo(finalFinish);
    assertThat(summary.getMaxMemoryQuery()).isEqualTo(maxMemoryQuery);
    assertThat(summary.getMaxPendingQuery()).isEqualTo(maxPendingQuery);
    assertThat(summary.getMaxAttemptsQuery()).isEqualTo(maxAttemptsQuery);
    assertThat(summary.getMaxCommandPoolQuery()).isEqualTo(maxCommandPoolQuery);
    assertThat(summary.getMaxMetadataRetrievalQuery()).isEqualTo(maxMetadataQuery);
  }

  @Test
  void testGenerateBucketsWithOneBucket() {
    Map<Long, List<SummaryQuery>> buckets = SummaryAggregator.makeQueryBuckets(1L, 100L);
    assertThat(buckets.size()).isEqualTo(1);
    // rounds down so will be zero
    assertThat(buckets.get(0L)).isEqualTo(new ArrayList<>());
    // next try with a second more
    buckets = SummaryAggregator.makeQueryBuckets(1001L, 1100L);
    System.out.println(buckets);
    assertThat(buckets.get(1000L)).isEqualTo(new ArrayList<>());
  }

  @Test
  void fillBucketsWhenQueryIsInOnlyBucket() {
    Query query = new Query();
    query.setStart(1002L);
    query.setFinish(1004L);
    query.setQueryText("select * from bar");
    SummaryAggregator summaryAggregator = new SummaryAggregator(Stream.of(query));
    Summary summary = new Summary();
    summary.setStart(1000L);
    summary.setFinish(2000L);
    summaryAggregator.fillBuckets(summary);
    assertThat(summary.getBuckets().size()).isEqualTo(1);
    assertThat(summary.getBusiestBucket().getQueries().size()).isEqualTo(1);
  }

  @Test
  void fillBucketsWhenQueryMultipleBuckets() {
    Query query = new Query();
    query.setStart(1002L);
    query.setFinish(2004L);
    query.setQueryText("select * from bar");
    SummaryAggregator summaryAggregator = new SummaryAggregator(Stream.of(query));
    Summary summary = new Summary();
    summary.setStart(0L);
    summary.setFinish(1000L * 10);
    summaryAggregator.fillBuckets(summary);
    assertThat(summary.getBuckets().size()).isEqualTo(10);
    assertThat(summary.getBuckets().get(0L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(1000L).size()).isEqualTo(1);
    assertThat(summary.getBuckets().get(2000L).size()).isEqualTo(1);
    assertThat(summary.getBuckets().get(3000L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(4000L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(5000L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(6000L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(7000L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(8000L).size()).isEqualTo(0);
    assertThat(summary.getBuckets().get(9000L).size()).isEqualTo(0);
    assertThat(summary.getBusiestBucket().getQueries().size()).isEqualTo(1);
  }
}
