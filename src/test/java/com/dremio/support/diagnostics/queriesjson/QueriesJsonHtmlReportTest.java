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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueriesJsonHtmlReportTest {

  private final Instant noOpStart = Instant.parse("1970-01-01T00:00:00.000Z");
  private final Instant noOpEnd = Instant.parse("2970-01-01T00:00:00.000Z");

  static String htmlText;

  @BeforeEach
  void setup() {
    long startTime = 1664362915 * 1000L;
    long finishTime = startTime + 1000L;
    long metadataRetrieval = 1000L;
    long attemptCount = 1999L;

    Query dropQueryThatLastsOneSecond = new Query();
    dropQueryThatLastsOneSecond.setStart(startTime);
    dropQueryThatLastsOneSecond.setFinish(finishTime);
    dropQueryThatLastsOneSecond.setQueryText("DROP TABLE TEST");
    dropQueryThatLastsOneSecond.setMetadataRetrieval(metadataRetrieval);
    dropQueryThatLastsOneSecond.setAttemptCount(attemptCount);
    dropQueryThatLastsOneSecond.setPendingTime(10L * 1000);
    dropQueryThatLastsOneSecond.setPoolWaitTime(10000L);

    Query queryThatIsThreeSecondsLater = new Query();
    queryThatIsThreeSecondsLater.setStart(startTime + 3001L);
    queryThatIsThreeSecondsLater.setFinish(finishTime + 3000L);
    queryThatIsThreeSecondsLater.setQueryText("SELECT * FROM TEST2");
    queryThatIsThreeSecondsLater.setMetadataRetrieval(0L);
    queryThatIsThreeSecondsLater.setAttemptCount(0L);
    queryThatIsThreeSecondsLater.setPendingTime(0L);
    queryThatIsThreeSecondsLater.setPoolWaitTime(0L);
    List<Query> queries = Arrays.asList(dropQueryThatLastsOneSecond, queryThatIsThreeSecondsLater);
    var report = new QueriesJsonHtmlReport(5, queries.stream(), 100000L, 3600L, noOpStart, noOpEnd);
    htmlText = report.getText();
  }

  @Test
  public void testQueryCountIsGenerated() {
    // verify that the series is being generated (this matches the query count)
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:55Z\",\"2022-09-28T11:01:56Z\",\"2022-09-28T11:01:57Z\",\"2022-09-28T11:01:58Z\"],"
                + "y:[1,1,0,1]," // note the empty time in the third
                // second
                + "mode: 'lines',"
                + "xaxis: 'x',"
                + "yaxis: 'y',"
                + "type: 'scatter',"
                + "name: 'all queries'");
  }

  @Test
  public void testSchemaQueriesAreCounted() {
    // verify the schema/refresh query is charted
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:55Z\",\"2022-09-28T11:01:56Z\",\"2022-09-28T11:01:57Z\",\"2022-09-28T11:01:58Z\"],y:[1,1,0,0],mode:"
                + " 'lines',xaxis: 'x',yaxis: 'y',type: 'scatter',name: 'refresh, drop, alter,"
                + " create queries'");
  }

  @Test
  public void testMaxPendingAreCounted() {
    // verify that max pending is charted
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:55Z\",\"2022-09-28T11:01:56Z\",\"2022-09-28T11:01:57Z\",\"2022-09-28T11:01:58Z\"],y:[10,10,0,0],mode:"
                + " 'lines',xaxis: 'x',yaxis: 'y',type: 'scatter',name: 'max seconds pending"
                + " time'");
  }

  @Test
  public void testMaxAttemptsAreCounted() {
    // verify that maxattempts is charted
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:55Z\",\"2022-09-28T11:01:56Z\",\"2022-09-28T11:01:57Z\",\"2022-09-28T11:01:58Z\"],y:[1999,1999,0,0],mode:"
                + " 'lines',xaxis: 'x',yaxis: 'y',type: 'scatter',name: 'max attempts'");
  }

  @Test
  public void testMaxQueuesResultsAreCounted() {
    // verify that the max queued is charted
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:55Z\",\"2022-09-28T11:01:56Z\",\"2022-09-28T11:01:57Z\",\"2022-09-28T11:01:58Z\"],y:[0,0,0,0],mode:"
                + " 'lines',xaxis: 'x',yaxis: 'y',type: 'scatter',name: 'max seconds queued'");
  }

  @Test
  public void testMemoryAllocatedIsCharted() {
    // verify that the memory allocated max is charted
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:55Z\",\"2022-09-28T11:01:56Z\",\"2022-09-28T11:01:57Z\",\"2022-09-28T11:01:58Z\"],y:[0,0,0,0],mode:"
                + " 'lines',xaxis: 'x',yaxis: 'y',type: 'scatter',name: 'bytes allocated'");
  }

  @Test
  public void testNoQueries() {
    List<Query> emptyList = Collections.emptyList();
    QueriesJsonHtmlReport report =
        new QueriesJsonHtmlReport(5, emptyList.stream(), 10000L, 4000, noOpStart, noOpEnd);
    assertThat(report.getText()).isEqualTo("no queries found");
  }

  @Test
  public void testCalculateMaxMemoryUsageOverDurationOfQuery() {
    Query query = new Query();
    query.setMemoryAllocated(10000L * 1048576); // 1048576 is megabytes);
    query.setStart(0L);
    query.setFinish(10000L);
    query.setQueryText("select * from foo");
    QueriesJsonHtmlReport report =
        new QueriesJsonHtmlReport(5, Stream.of(query), 10000L, 3600, noOpStart, noOpEnd);
    String longMemoryAllocatedHtml = report.getText();
    // note we divide the memory into 'by second' each bucket has 1000.0 memory
    // allocated
    assertThat(longMemoryAllocatedHtml)
        .contains(
            "x:[\"1970-01-01T00:00:00Z\",\"1970-01-01T00:00:01Z\",\"1970-01-01T00:00:02Z\",\"1970-01-01T00:00:03Z\",\"1970-01-01T00:00:04Z\",\"1970-01-01T00:00:05Z\",\"1970-01-01T00:00:06Z\",\"1970-01-01T00:00:07Z\",\"1970-01-01T00:00:08Z\",\"1970-01-01T00:00:09Z\"],y:[1000,1000,1000,1000,1000,1000,1000,1000,1000,1000],mode:"
                + " 'lines',xaxis: 'x',yaxis: 'y',type: 'scatter',name: 'bytes allocated'");
  }

  @Test
  public void testAllKnownSchemaAndRefreshTypes() {
    Query query1 = new Query();
    query1.setStart(0L);
    query1.setFinish(999L);
    query1.setQueryText("DROP TABLE FOO");
    Query query2 = new Query();
    query2.setStart(0L);
    query2.setFinish(999L);
    query2.setQueryText("CREATE TABLE FOO");
    Query query3 = new Query();
    query3.setStart(0L);
    query3.setFinish(999L);
    query3.setQueryText("REFRESH DATASET");

    Query query4 = new Query();
    query4.setStart(0L);
    query4.setFinish(999L);
    query4.setQueryText("ALTER TABLE");

    QueriesJsonHtmlReport report =
        new QueriesJsonHtmlReport(
            5, Stream.of(query1, query2, query3, query4), 10000L, 3600, noOpStart, noOpEnd);
    String generatedHtml = report.getText();
    // should see all 4 types of queries in the count
    assertThat(generatedHtml)
        .contains(
            "x:[\"1970-01-01T00:00:00Z\"],"
                + "y:[4],"
                + "mode: 'lines',"
                + "xaxis: 'x',"
                + "yaxis: 'y',"
                + "type: 'scatter',"
                + "name: 'refresh, drop, alter, create queries'");
  }

  @Test
  public void testWithEngineNameHavingBogusCharacters() {

    long startTime = 1664362915 * 1000L;
    long finishTime = startTime + 1000L;
    long metadataRetrieval = 1000L;
    long attemptCount = 1999L;

    Query query = new Query();
    query.setStart(startTime);
    query.setFinish(finishTime);
    query.setQueryText("DROP TABLE TEST");
    query.setEngineName("ABC[11](22)");
    query.setMetadataRetrieval(metadataRetrieval);
    query.setAttemptCount(attemptCount);
    query.setPendingTime(10L * 1000);
    query.setPoolWaitTime(10000L);
    Query query2 = new Query();
    query2.setStart(startTime + 3000L);
    query2.setFinish(finishTime + 3000L);
    query2.setQueryText("SELECT * FROM TEST2");
    query2.setEngineName("ABC[21](22)");
    query2.setMetadataRetrieval(0L);
    query2.setAttemptCount(0L);
    query2.setPendingTime(0L);
    query2.setPoolWaitTime(0L);
    List<Query> queries = Arrays.asList(query, query2);
    htmlText =
        new QueriesJsonHtmlReport(5, queries.stream(), 100000L, 3600, noOpStart, noOpEnd).getText();

    assertThat(htmlText).contains("var engineNameABC_11__22_");
  }

  @Test
  public void testFilterOut() {
    long startTime = 1664362915 * 1000L;
    long finishTime = startTime + 1000L;
    long metadataRetrieval = 1000L;
    long attemptCount = 1999L;

    Query dropQueryThatLastsOneSecond = new Query();
    dropQueryThatLastsOneSecond.setStart(startTime);
    dropQueryThatLastsOneSecond.setFinish(finishTime);
    dropQueryThatLastsOneSecond.setQueryText("DROP TABLE TEST");
    dropQueryThatLastsOneSecond.setMetadataRetrieval(metadataRetrieval);
    dropQueryThatLastsOneSecond.setAttemptCount(attemptCount);
    dropQueryThatLastsOneSecond.setPendingTime(10L * 1000);
    dropQueryThatLastsOneSecond.setPoolWaitTime(10000L);

    Query queryThatIsThreeSecondsLater = new Query();
    queryThatIsThreeSecondsLater.setStart(startTime + 3001L);
    queryThatIsThreeSecondsLater.setFinish(finishTime + 3000L);
    queryThatIsThreeSecondsLater.setQueryText("SELECT * FROM TEST2");
    queryThatIsThreeSecondsLater.setMetadataRetrieval(0L);
    queryThatIsThreeSecondsLater.setAttemptCount(0L);
    queryThatIsThreeSecondsLater.setPendingTime(0L);
    queryThatIsThreeSecondsLater.setPoolWaitTime(0L);
    // this should filter out the first query
    final Instant startFilter = Instant.parse("2022-09-28T11:01:56.000Z");
    final Instant endFilter = Instant.parse("2072-09-28T11:01:56.000Z");
    List<Query> queries = Arrays.asList(dropQueryThatLastsOneSecond, queryThatIsThreeSecondsLater);
    var report =
        new QueriesJsonHtmlReport(5, queries.stream(), 100000L, 3600L, startFilter, endFilter);
    htmlText = report.getText();
    assertThat(htmlText)
        .contains(
            "x:[\"2022-09-28T11:01:58Z\"],"
                + "y:[1]," // note the empty time in the third second
                + "mode: 'lines',"
                + "xaxis: 'x',"
                + "yaxis: 'y',"
                + "type: 'scatter',"
                + "name: 'all queries'");
  }
}
