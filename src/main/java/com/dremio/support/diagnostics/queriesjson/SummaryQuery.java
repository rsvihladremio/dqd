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

/** used in the summary text of queries for performance reasons since it is only three fields. */
public class SummaryQuery {
  private String queryText;
  private long finishEpochMillis;
  private long startEpochMillis;

  /**
   * get the SQL of the query
   *
   * @return query text in SQL format
   */
  public String getQueryText() {
    return queryText;
  }

  /**
   * set text of the query
   *
   * @param queryText SQL query text
   */
  public void setQueryText(String queryText) {
    this.queryText = queryText;
  }

  /**
   * get in epoch time using milliseconds when did the query finish
   *
   * @return epoch time milliseconds
   */
  public long getFinishEpochMillis() {
    return finishEpochMillis;
  }

  /**
   * set epoch time in milliseconds when the query finished
   *
   * @param finishEpochMillis epoch time in milliseconds
   */
  public void setFinishEpochMillis(long finishEpochMillis) {
    this.finishEpochMillis = finishEpochMillis;
  }

  /**
   * get in epoch time in milliseconds when the query started
   *
   * @return epoch time in milliseconds
   */
  public long getStartEpochMillis() {
    return startEpochMillis;
  }

  /**
   * set epoch time in milliseconds when the query started
   *
   * @param startEpochMillis epoch time in milliseconds
   */
  public void setStartEpochMillis(long startEpochMillis) {
    this.startEpochMillis = startEpochMillis;
  }
}
