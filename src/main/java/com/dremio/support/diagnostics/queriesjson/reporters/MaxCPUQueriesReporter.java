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

public class MaxCPUQueriesReporter implements QueryReporter {
  private List<Query> queries = new ArrayList<>();

  public List<Query> getQueries() {
    return queries;
  }

  private final long limit;

  public MaxCPUQueriesReporter(final long limit) {
    this.limit = limit;
  }

  @Override
  public synchronized void parseRow(Query q) {
    this.queries.add(q);
    // need to make sure use an array list to make this writeable again since toList makes it
    // immutable
    this.queries =
        new ArrayList<>(
            this.queries.stream()
                .sorted(Comparator.comparingLong(Query::getExecutionCpuTimeNs).reversed())
                .limit(this.limit)
                .toList());
  }
}
