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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Summary {
  private long start;
  private long finish;

  public Query getMaxMemoryQuery() {
    return maxMemoryQuery;
  }

  public void setMaxMemoryQuery(Query maxMemoryQuery) {
    this.maxMemoryQuery = maxMemoryQuery;
  }

  public Query getMaxMetadataRetrievalQuery() {
    return maxMetadataRetrievalQuery;
  }

  public void setMaxMetadataRetrievalQuery(Query maxMetadataRetrievalQuery) {
    this.maxMetadataRetrievalQuery = maxMetadataRetrievalQuery;
  }

  public Query getMaxAttemptsQuery() {
    return maxAttemptsQuery;
  }

  public void setMaxAttemptsQuery(Query maxAttemptsQuery) {
    this.maxAttemptsQuery = maxAttemptsQuery;
  }

  public Query getMaxPendingQuery() {
    return maxPendingQuery;
  }

  public void setMaxPendingQuery(Query maxPendingQuery) {
    this.maxPendingQuery = maxPendingQuery;
  }

  public Query getMaxCommandPoolQuery() {
    return maxCommandPoolQuery;
  }

  public void setMaxCommandPoolQuery(Query maxCommandPoolQuery) {
    this.maxCommandPoolQuery = maxCommandPoolQuery;
  }

  private Query maxMemoryQuery;
  private Query maxMetadataRetrievalQuery;
  private Query maxAttemptsQuery;
  private Query maxPendingQuery;
  private Query maxCommandPoolQuery;

  public Map<Long, List<SummaryQuery>> getBuckets() {
    return buckets;
  }

  public void setBuckets(Map<Long, List<SummaryQuery>> buckets) {
    this.buckets = buckets;
  }

  private Map<Long, List<SummaryQuery>> buckets;

  public void setStart(long start) {
    this.start = start;
  }

  public long getStart() {
    return start;
  }

  public void setFinish(long finish) {
    this.finish = finish;
  }

  public long getFinish() {
    return finish;
  }

  public Bucket getBusiestBucket() {
    Map.Entry<Long, List<SummaryQuery>> busiestBucket =
        new AbstractMap.SimpleEntry<>(0L, new ArrayList<>());
    for (Map.Entry<Long, List<SummaryQuery>> kvp : this.getBuckets().entrySet()) {
      if (kvp.getValue().size() > busiestBucket.getValue().size()) {
        busiestBucket = kvp;
      }
    }
    Bucket bucket = new Bucket();
    bucket.setTimestamp(busiestBucket.getKey());
    bucket.setQueries(busiestBucket.getValue());
    return bucket;
  }
}
