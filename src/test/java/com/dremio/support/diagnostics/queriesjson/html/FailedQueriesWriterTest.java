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
package com.dremio.support.diagnostics.queriesjson.html;

import static org.junit.jupiter.api.Assertions.*;

import com.dremio.support.diagnostics.queriesjson.Query;
import com.dremio.support.diagnostics.shared.Human;
import java.util.ArrayList;
import java.util.List;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.junit.jupiter.api.Test;

class FailedQueriesWriterTest {

  @Test
  void generateTable() {
    final List<Query> queries = new ArrayList<>();
    final Query q1 = new Query();
    q1.setQueryId("123");
    q1.setStart(1721129988000L);
    q1.setFinish(1721130003000L);
    q1.setQueryText("SELECT * FROM FOO");
    q1.setOutcome("FAILED");
    q1.setOutcomeReason("I don't want to");
    q1.setQueueName("BI");
    final long expectedPendingTime1 = 19889L;
    q1.setPendingTime(expectedPendingTime1);
    final long expectedMetadataRetrieval = 8908908L;
    q1.setMetadataRetrieval(expectedMetadataRetrieval);
    final long expectedPlanningTime1 = 121234L;
    q1.setPlanningTime(expectedPlanningTime1);
    final long expectedQueueTime1 = 12098098L;
    q1.setQueuedTime(expectedQueueTime1);
    queries.add(q1);
    final long expectedRunTime1 = 12345L;
    q1.setRunningTime(expectedRunTime1);
    final Query q2 = new Query();
    q2.setQueryId("456");
    q2.setStart(1721129988000L + 10);
    q2.setFinish(1721130003000L - 5);
    q2.setQueryText("SELECT * FROM BAR");
    q2.setOutcome("FAILED");
    q2.setOutcomeReason("it was silly");
    q2.setQueueName("CI");
    final long expectedPendingTime2 = 919889L;
    q2.setPendingTime(expectedPendingTime2);
    final long expectedMetadataRetrieval2 = 98908908L;
    q2.setMetadataRetrieval(expectedMetadataRetrieval2);
    final long expectedPlanningTime2 = 9121234L;
    q2.setPlanningTime(expectedPlanningTime2);
    final long expectedQueueTime2 = 912098098L;
    q2.setQueuedTime(expectedQueueTime2);
    queries.add(q2);
    final String text = FailedQueriesWriter.generateTable(queries, 1);
    Document doc = Jsoup.parseBodyFragment(text);
    Elements failureQueries = doc.select("#firstFailedQueries tbody tr");
    // should match both queries
    assertEquals(failureQueries.size(), 2, text);
    Elements allColumns = failureQueries.get(0).select("td");
    assertEquals(q1.getQueryId(), allColumns.get(0).text());
    assertEquals("Tue, 16 Jul 2024 11:39:48 GMT", allColumns.get(1).text());
    assertEquals(Long.toString(q1.getStart()), allColumns.get(1).attr("data-sort"));
    assertEquals("Tue, 16 Jul 2024 11:40:03 GMT", allColumns.get(2).text());
    assertEquals(Long.toString(q1.getFinish()), allColumns.get(2).attr("data-sort"));
    assertEquals("15.00 seconds", allColumns.get(3).text());
    // check sort of duration
    assertEquals(Long.toString(15000), allColumns.get(3).attr("data-sort"));
    assertEquals("SELECT * FROM FOO", allColumns.get(4).text());
    assertEquals("I don't want to", allColumns.get(5).text());
    assertEquals("BI", allColumns.get(6).text());
    // pending time
    assertEquals("19.89 seconds", allColumns.get(7).text());
    // check sort of pending
    assertEquals(Long.toString(q1.getPendingTime()), allColumns.get(7).attr("data-sort"));
    // metadata retrieval time
    assertEquals("2.47 hours", allColumns.get(8).text());
    assertEquals(
        Human.getHumanDurationFromMillis(q1.getNormalizedMetadataRetrieval()),
        allColumns.get(8).text());
    // sort of metadata retrieval time
    assertEquals(
        Long.toString(q1.getNormalizedMetadataRetrieval()), allColumns.get(8).attr("data-sort"));

    // planning time
    assertEquals("2.02 minutes", allColumns.get(9).text());
    assertEquals(Human.getHumanDurationFromMillis(q1.getPlanningTime()), allColumns.get(9).text());
    // sort of planning time
    assertEquals(Long.toString(q1.getPlanningTime()), allColumns.get(9).attr("data-sort"));

    // queued time
    assertEquals("3.36 hours", allColumns.get(10).text());
    assertEquals(Human.getHumanDurationFromMillis(q1.getQueuedTime()), allColumns.get(10).text());
    // sort of queued time
    assertEquals(Long.toString(q1.getQueuedTime()), allColumns.get(10).attr("data-sort"));

    // running time
    assertEquals("12.35 seconds", allColumns.get(11).text());
    assertEquals(Human.getHumanDurationFromMillis(q1.getRunningTime()), allColumns.get(11).text());
    // sort of running time
    assertEquals(Long.toString(q1.getRunningTime()), allColumns.get(11).attr("data-sort"));
  }
}
