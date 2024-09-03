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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FailedQueriesReporter implements QueryReporter {
  private final long limit;
  private List<Query> failedQueries = new ArrayList<>();

  public FailedQueriesReporter(final long limit) {
    this.limit = limit;
  }

  @Override
  public synchronized void parseRow(Query q) {
    if ("FAILED".equals(q.getOutcome())) {
      failedQueries.add(q);
      // we want to get the oldest based on start time LIMIT failed queries
      failedQueries =
          failedQueries.stream()
              .sorted(Comparator.comparingLong(Query::getStart))
              .limit(this.limit)
              .collect(Collectors.toList());
    }
  }

  public synchronized Collection<Query> getFailedQueries() {
    return this.failedQueries;
  }
}
