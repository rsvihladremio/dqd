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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BucketGraphTest {
  @Test
  public void testNoBuckets() {
    long start = 1666012020L * 1000;
    // same start and finish
    BucketGraph bucketGraph = new BucketGraph(start, start);
    List<Query> queries = new ArrayList<>();
    Aggregator aggregator = (query, values, index) -> {};
    DataPoints filteredPerSecondBuckets =
        bucketGraph.getFilteredBuckets(queries, x -> true, aggregator);
    assertThat(filteredPerSecondBuckets.getTimestamps().length)
        .isEqualTo(0); // one day worth of buckets
    assertThat(filteredPerSecondBuckets.getValues().length)
        .isEqualTo(0); // one day worth of buckets
  }

  @Test
  public void testOneBucket() {
    long start = 1666012020L * 1000;
    long finish = (1666012020 + 1) * 1000L;
    BucketGraph bucketGraph = new BucketGraph(start, finish);
    List<Query> queries = new ArrayList<>();
    Aggregator aggregator = (query, values, index) -> {};
    DataPoints filteredPerSecondBuckets =
        bucketGraph.getFilteredBuckets(queries, x -> true, aggregator);
    assertThat(filteredPerSecondBuckets.getTimestamps().length)
        .isEqualTo(1); // one day worth of buckets
    assertThat(filteredPerSecondBuckets.getValues().length)
        .isEqualTo(1); // one day worth of buckets
  }

  @Test
  public void testBucketGraphFor1Day() {
    long start = 1666012020L * 1000;
    long finish = (1666012020 + 86400) * 1000L;
    BucketGraph bucketGraph = new BucketGraph(start, finish);
    List<Query> queries = new ArrayList<>();
    Aggregator aggregator = (query, values, index) -> {};
    DataPoints filteredPerSecondBuckets =
        bucketGraph.getFilteredBuckets(queries, x -> true, aggregator);
    assertThat(filteredPerSecondBuckets.getTimestamps().length)
        .isEqualTo(24 * 60 * 60); // one day worth of buckets
    assertThat(filteredPerSecondBuckets.getValues().length)
        .isEqualTo(24 * 60 * 60); // one day worth of buckets
  }

  @Test
  public void testBucketGraphFor1DayAnd1Second() {
    long start = 1666012020 * 1000L;
    long finish = start + (86401 * 1000L);
    BucketGraph bucketGraph = new BucketGraph(start, finish);
    List<Query> queries = new ArrayList<>();
    Aggregator aggregator = (query, values, index) -> {};
    DataPoints filteredPerSecondBuckets =
        bucketGraph.getFilteredBuckets(queries, x -> true, aggregator);
    assertThat(filteredPerSecondBuckets.getTimestamps().length)
        .isEqualTo((24 * 60 * 60) + 1); // one day worth of buckets
    assertThat(filteredPerSecondBuckets.getValues().length)
        .isEqualTo((24 * 60 * 60) + 1); // one day worth of buckets
  }

  @Test
  public void testPartialSecondIsFlattenedToWholeSecond() {
    long epochSecond = 1666012061L;
    long epochMillis = epochSecond * 1000L;
    long epochMillisPlusHalfSecond = epochMillis + 1L;
    BucketGraph bucketGraph =
        new BucketGraph(epochMillisPlusHalfSecond, epochMillisPlusHalfSecond + 1L);
    long actualStartTime = bucketGraph.getStartTime();
    long actualFinishTime = bucketGraph.getFinishTime();
    DataPoints dataPoints =
        bucketGraph.getFilteredBuckets(Collections.emptyList(), x -> true, (x, y, z) -> {});
    long[] timestamps = dataPoints.getTimestamps();
    assertThat(timestamps.length).isEqualTo(1L);
    assertThat(actualStartTime).isEqualTo(epochMillis);
    assertThat(actualFinishTime).isEqualTo(epochMillis + 1000L);
  }

  @Test
  public void testFinishShouldBeRounded2Up() {
    long epochSecond = 1666012061L;
    long epochMillis = epochSecond * 1000L;
    long epochMillisPlusHalfSecond = epochMillis + 999L;
    BucketGraph bucketGraph =
        new BucketGraph(epochMillisPlusHalfSecond, epochMillisPlusHalfSecond + 2L);
    long actualStartTime = bucketGraph.getStartTime();
    long actualFinishTime = bucketGraph.getFinishTime();
    DataPoints dataPoints =
        bucketGraph.getFilteredBuckets(Collections.emptyList(), x -> true, (x, y, z) -> {});
    long[] timestamps = dataPoints.getTimestamps();
    assertThat(timestamps.length).isEqualTo(2L);
    assertThat(actualStartTime).isEqualTo(epochMillis);
    assertThat(actualFinishTime)
        .isEqualTo(epochMillis + 2000L); // because it spans 2 time windows it will rond up
  }
}
