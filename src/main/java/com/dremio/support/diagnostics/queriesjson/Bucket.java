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

import java.util.*;

/** Bucket contains the timestamp measured and all queries that crossed that bucket */
public class Bucket {
  private long timestamp;
  private List<SummaryQuery> queries;

  /**
   * get the timestamp in epoch time milliseconds
   *
   * @return epoch time in milliseconds
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * set the timestamp in epoch time milliseconds
   *
   * @param timestamp epoch time in milliseconds
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * the queries associated with the bucket, the bucket time should be between start and end time,
   * if not this is a fundamental error
   *
   * @return list of queries that are associated with the bucket
   */
  public List<SummaryQuery> getQueries() {
    return queries;
  }

  /**
   * sets the queries associated with the bucket, the bucket time should be between start and end
   * time, if not this is a fundamental error
   *
   * @param queries list of queries that are associated with the bucket
   */
  public void setQueries(List<SummaryQuery> queries) {
    this.queries = queries;
  }
}
