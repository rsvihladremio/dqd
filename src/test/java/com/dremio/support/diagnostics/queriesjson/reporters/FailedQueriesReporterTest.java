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

import static org.junit.jupiter.api.Assertions.*;

import com.dremio.support.diagnostics.queriesjson.Query;
import java.util.Collection;
import org.junit.jupiter.api.Test;

class FailedQueriesReporterTest {

  @Test
  void getFailedQueries() {
    FailedQueriesReporter reporter = new FailedQueriesReporter(1);
    Query q1 = new Query();
    q1.setStart(100L);
    q1.setOutcome("FAILED");
    reporter.parseRow(q1);
    Query oldestFailedQuery = new Query();
    oldestFailedQuery.setStart(1L);
    oldestFailedQuery.setOutcome("FAILED");
    reporter.parseRow(oldestFailedQuery);
    Query q3 = new Query();
    q3.setOutcome("COMPLETED");
    reporter.parseRow(q3);
    Collection<Query> queries = reporter.getFailedQueries();
    assertEquals(1, queries.size());
    // with a limit of 1 it should be the oldest query
    assertEquals(oldestFailedQuery.getStart(), queries.stream().findFirst().get().getStart());
  }
}
