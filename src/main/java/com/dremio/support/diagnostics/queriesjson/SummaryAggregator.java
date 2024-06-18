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

import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SummaryAggregator {

  private final List<Query> list;

  public SummaryAggregator(Stream<Query> queries) {
    list = queries.sorted(Comparator.comparing(Query::getStart)).collect(Collectors.toList());
  }

  public Summary generateSummary() {
    Optional<Summary> result =
        list.stream()
            .parallel()
            .map(
                x -> {
                  Summary summary = new Summary();
                  summary.setStart(x.getStart());
                  summary.setFinish(x.getFinish());
                  summary.setMaxMetadataRetrievalQuery(x);
                  summary.setMaxAttemptsQuery(x);
                  summary.setMaxPendingQuery(x);
                  summary.setMaxCommandPoolQuery(x);
                  summary.setMaxMemoryQuery(x);
                  return summary;
                })
            .reduce(
                (a, b) -> {
                  if (a.getMaxMemoryQuery().getMemoryAllocated()
                      > b.getMaxMemoryQuery().getMemoryAllocated()) {
                    b.setMaxMemoryQuery(a.getMaxMemoryQuery());
                  }
                  if (a.getMaxMetadataRetrievalQuery().getNormalizedMetadataRetrieval()
                      > b.getMaxMetadataRetrievalQuery().getNormalizedMetadataRetrieval()) {
                    b.setMaxMetadataRetrievalQuery(a.getMaxMetadataRetrievalQuery());
                  }
                  if (a.getMaxAttemptsQuery().getAttemptCount()
                      > b.getMaxAttemptsQuery().getAttemptCount()) {
                    b.setMaxAttemptsQuery(a.getMaxAttemptsQuery());
                  }
                  if (a.getMaxPendingQuery().getPendingTime()
                      > b.getMaxPendingQuery().getPendingTime()) {
                    b.setMaxPendingQuery(a.getMaxPendingQuery());
                  }
                  if (a.getMaxCommandPoolQuery().getPoolWaitTime()
                      > b.getMaxCommandPoolQuery().getPoolWaitTime()) {
                    b.setMaxCommandPoolQuery(a.getMaxCommandPoolQuery());
                  }
                  if (a.getStart() < b.getStart()) {
                    b.setStart(a.getStart());
                  }
                  if (a.getFinish() > b.getFinish()) {
                    b.setFinish(a.getFinish());
                  }
                  return b;
                });
    if (!result.isPresent()) {
      throw new RuntimeException("unexpected empty summary");
    }
    Summary summary = result.get();
    // if the max value is still zero, null out the results so consumers can know
    // there was not true
    // maximum found in the queries.json
    if (summary.getMaxCommandPoolQuery().getPoolWaitTime() == 0) {
      summary.setMaxCommandPoolQuery(null);
    }
    if (summary.getMaxPendingQuery().getPendingTime() == 0) {
      summary.setMaxPendingQuery(null);
    }
    if (summary.getMaxMetadataRetrievalQuery().getNormalizedMetadataRetrieval() == 0) {
      summary.setMaxMetadataRetrievalQuery(null);
    }
    if (summary.getMaxMemoryQuery().getMemoryAllocated() == 0) {
      summary.setMaxMemoryQuery(null);
    }
    if (summary.getMaxAttemptsQuery().getAttemptCount() == 0) {
      summary.setMaxAttemptsQuery(null);
    }
    fillBuckets(summary);
    return summary;
  }

  public boolean isEmpty() {
    return list.isEmpty();
  }

  public int size() {
    return list.size();
  }

  public static Map<Long, List<SummaryQuery>> makeQueryBuckets(long start, long finish) {
    if (finish < start) {
      throw new InvalidParameterException(
          "start cannot be before finish. This is a critical error");
    }
    Map<Long, List<SummaryQuery>> buckets = new HashMap<>();
    start = Instant.ofEpochMilli(start).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
    while (start < finish) {
      buckets.put(start, new ArrayList<>());
      start += 1000;
    }
    return buckets;
  }

  void fillBuckets(Summary summary) {
    ExecutorService executorService = Executors.newFixedThreadPool(32);
    Map<Long, List<SummaryQuery>> buckets =
        makeQueryBuckets(summary.getStart(), summary.getFinish());

    List<Future<Boolean>> futures = new ArrayList<>();
    for (long bucket : buckets.keySet()) {
      for (Query query : list) {
        boolean queryStartsAfterBucket = query.getStart() > (bucket + 1000L);
        if (queryStartsAfterBucket) {
          // we can safely break because we sort the queries in the constructor
          break;
        }
        if (query.getFinish() < bucket) {
          continue;
        }
        SummaryQuery q = new SummaryQuery();
        q.setStartEpochMillis(query.getStart());
        q.setFinishEpochMillis(query.getFinish());
        q.setQueryText(query.getQueryText());
        Future<Boolean> future =
            executorService.submit(
                () -> {
                  synchronized (buckets) {
                    List<SummaryQuery> l = buckets.get(bucket);
                    l.add(q);
                    buckets.put(bucket, l);
                  }
                  return true;
                });
        futures.add(future);
      }
    }
    for (Future<Boolean> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    executorService.shutdown();
    summary.setBuckets(buckets);
  }

  public int getQueryCount() {
    return this.size();
  }
}
