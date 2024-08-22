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
package com.dremio.support.diagnostics.queriesjson.reporters;

import com.dremio.support.diagnostics.queriesjson.Query;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * finds the slowest metadata refresh queries
 */
public class SlowestMetadataQueriesReporter implements QueryReporter {
  private final long limit;
  private List<Query> queries = new ArrayList<>();

  /**
   *
   * @return the top <limit> queries ordered by getNormalizedMetadataRefresh() desc
   */
  public synchronized List<Query> getQueries() {
    return queries;
  }

  /**
   * finds the queries with the slowest medata refresh
   *
   * @param limit how many results to show
   */
  public SlowestMetadataQueriesReporter(final long limit) {
    this.limit = limit;
  }

  /**
   * @param q Query object to report on. thread safe. Is normalized to handle the change from
   * metadataRefreshTime and metadataRefresh
   */
  @Override
  public synchronized void parseRow(final Query q) {
    queries.add(q);
    // we need to new up the ArrayList<>() as .toList() makes the array unmodifiable.
    queries =
        new ArrayList<>(
            queries.stream()
                .sorted(Comparator.comparingLong(Query::getNormalizedMetadataRetrieval).reversed())
                .limit(limit)
                .toList());
  }
}
