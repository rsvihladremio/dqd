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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.*;
import java.util.logging.Logger;

/** BucketGraph generates second long buckets */
public class BucketGraph implements Graph {
  private static final Logger logger = Logger.getLogger(BucketGraph.class.getName());
  private final long[] timestamps;
  private final long[] values;
  private final long startTime;
  private final long finishTime;
  private final long bucketSizeMillis;

  /**
   * will generate a series of buckets between start and finish with each bucket being 1 second in
   * size. This does mean if the time range if big enough this operation can become very
   * computationally expensive.
   *
   * @param start start time in epoch millis which is the scale that queries.json uses
   * @param finish finish time in epoch millis which is the scale that queries.json uses
   */
  public BucketGraph(long start, long finish) {
    this(start, finish, 1000L);
  }

  /**
   * will generate a series of buckets between start and finish with each bucket being 1 second in
   * size. This does mean if the time range if big enough this operation can become very
   * computationally expensive.
   *
   * @param start start time in epoch millis which is the scale that queries.json uses
   * @param finish finish time in epoch millis which is the scale that queries.json uses
   * @param bucketSizeMillis this will generate buckets of this size
   */
  public BucketGraph(long start, long finish, long bucketSizeMillis) {
    this.bucketSizeMillis = bucketSizeMillis;
    // bucketed down to a second to give us a full bucket to start from
    start = Instant.ofEpochMilli(start).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
    // now it is safe to set the start time for the class so the bucket start and the bucket end
    // line up
    this.startTime = start;
    int counter = 0; // start with -1 or we get an extra bucket
    long tempStart = start;
    // figure out the array size by calculating how many 1 second buckets we would have
    while (tempStart < finish) {
      // buckets.put(start, 0L);
      counter += 1;
      tempStart += bucketSizeMillis;
    }
    // initialize the arrays
    timestamps = new long[counter];
    values = new long[counter];
    for (int i = 0; i < counter; i++) {
      // now we can start incrementing the start time for real
      timestamps[i] = start;
      values[i] = 0L;
      start += bucketSizeMillis;
    }

    // now it is safe to set the finish time to the last bucket finish + bucket time
    if (timestamps.length > 0) {
      this.finishTime = timestamps[timestamps.length - 1] + bucketSizeMillis;
    } else {
      this.finishTime = finish;
    }
  }

  @Override
  public long getBucketSizeMillis() {
    return this.bucketSizeMillis;
  }

  /**
   * getFilteredBuckets filters out queries by the predicate filter and for the remaining
   * queries passes them to the aggregator to perform calculation on the results, finally returns
   * the results which are grouped by second. The key magic that happens here is if a query passes
   * several buckets it will be counted in ALL buckets and not just the one where the query started
   * or ended. For dremio this is critical as queries can last a long time. However, since this is
   * based on queries.json it will not count queries that have not yet been written to the file, but
   * may still be in memory executing.
   *
   * @param queries queries to analyze
   * @param filter filter to remove certain queries from the dataset
   * @param mapper aggregator responsible for performing calculations on the data
   * @return the result of the filtering
   */
  @Override
  public final DataPoints getFilteredBuckets(
      final List<Query> queries, final Predicate<Query> filter, final Aggregator mapper) {
    // so we do not have to recalculate buckets over we do a shallow copy (which is good enough) of
    // the original buckets
    final long[] filteredValues = values.clone();
    long bucketTimestamp;
    long bucketEndTime;
    for (final Query query : queries) {
      for (int i = 0; i < timestamps.length; i++) {
        bucketTimestamp = timestamps[i];
        bucketEndTime = bucketTimestamp + this.bucketSizeMillis;
        if (!filter.test(query)) {
          continue;
        }
        if (query.getStart() > bucketEndTime) {
          continue;
        }
        if (query.getFinish() < bucketTimestamp) {
          continue;
        }

        mapper.agg(query, filteredValues, i);
      }
    }
    final long totalValue = Arrays.stream(filteredValues).reduce(0, Long::sum);
    // logging output for debugging
    logger.finer(
        () -> {
          StringBuilder builder = new StringBuilder();
          if (timestamps.length > 0) {
            builder.append("bucket start ");
            builder.append(Instant.ofEpochMilli(timestamps[0]));
            builder.append("\n");
            builder.append("bucket end ");
            builder.append(Instant.ofEpochMilli(timestamps[timestamps.length - 1]));
            builder.append("\n");
          }
          if (!queries.isEmpty()) {
            // safe to get first query now
            final Query firstQuery = queries.get(0);
            final long startTime = firstQuery.getStart();
            builder.append("query start ");
            builder.append(Instant.ofEpochMilli(startTime));
            builder.append("\n");
            final Query lastQuery = queries.get(queries.size() - 1);
            final long startTimeOfFinalQuery = lastQuery.getStart();
            builder.append("query last start ");
            builder.append(Instant.ofEpochMilli(startTimeOfFinalQuery));
            builder.append("\n");
          } else {
            builder.append("no queries for bucket\n");
          }
          builder.append("total value of ");
          builder.append(totalValue);
          builder.append("\n");
          return builder.toString();
        });
    final DataPoints datapoint = new DataPoints();
    datapoint.setTimestamps(timestamps);
    datapoint.setValues(filteredValues);
    return datapoint;
  }

  /**
   * simple no op calculation for when we want no filtering
   *
   * @param ignoredQ not used
   * @return always true
   */
  public static boolean noOp(Query ignoredQ) {
    return true;
  }

  /**
   * getter for start time
   *
   * @return start time in epoch millis
   */
  public final long getStartTime() {
    return startTime;
  }

  /**
   * getter for finish time
   *
   * @return end time in epoch millis
   */
  public final long getFinishTime() {
    return finishTime;
  }
}
