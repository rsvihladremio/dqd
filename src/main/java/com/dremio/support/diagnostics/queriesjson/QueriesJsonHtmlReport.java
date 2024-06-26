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

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;
import static java.util.Arrays.asList;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import com.dremio.support.diagnostics.queriesjson.html.ConcurrentQueueWriter;
import com.dremio.support.diagnostics.queriesjson.html.Dates;
import com.dremio.support.diagnostics.queriesjson.html.MaxMemoryQueriesWriter;
import com.dremio.support.diagnostics.queriesjson.html.MaxTimeWriter;
import com.dremio.support.diagnostics.queriesjson.html.MemoryAllocatedWriter;
import com.dremio.support.diagnostics.queriesjson.html.RequestByQueueWriter;
import com.dremio.support.diagnostics.queriesjson.html.RequestCounterWriter;
import com.dremio.support.diagnostics.queriesjson.html.SlowestMetadataRetrievalWriter;
import com.dremio.support.diagnostics.queriesjson.html.SlowestPlanningWriter;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import com.dremio.support.diagnostics.shared.Report;

public class QueriesJsonHtmlReport implements Report {
  private static final Logger LOGGER = Logger.getLogger(QueriesJsonHtmlReport.class.getName());
  private final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();
  private Instant start;
  private Instant end;
  private final long totalQueries;
  private final Map<String, Long> requestCounterMap;
  private final Map<String, Long> requestsByQueue;
  private final Map<Long, Double> memoryUsage;
  private final Collection<Query> slowestPlanning;
  private final Collection<Query> slowestMetadata;
  private final long bucketSize;
  private final Map<Long, Long> maxPending;
  private final Map<Long, Long> maxAttemps;
  private final Map<Long, Long> maxQueued;
  private final Map<Long, Long> maxPlanning;
  private final Map<Long, Long> totalQueryCounts;
  private final Map<Long, Long> schemaOpsCounts;
  private final Map<String, Map<Long, Long>> queueCounts;

  private Collection<Query> mostMemoryQueries;

  public QueriesJsonHtmlReport(
      final long bucketSize,
      final long totalQueries,
      final Collection<Query> slowestPlanning,
      final Collection<Query> slowestMetadata,
      final Collection<Query> mostMemoryQueries,
      final Map<String, Long> requestCounterMap,
      final Map<String, Long> requestsByQueue,
      final Map<Long, Double> memoryUsage,
      final Map<Long, Long> maxPending,
      final Map<Long, Long> maxAttemps,
      final Map<Long, Long> maxQueued,
      final Map<Long, Long> maxPlanning,
      final  Map<Long, Long> totalQueryCounts,
      final  Map<Long, Long> schemaOpsCounts,
      final Map<String, Map<Long, Long>> queueCounts, 
      final Instant start,
      final Instant end) {
    this.bucketSize = bucketSize;
    this.totalQueries = totalQueries;
    this.mostMemoryQueries = mostMemoryQueries;
    this.maxPending = maxPending;
    this.maxAttemps = maxAttemps;
    this.maxQueued = maxQueued;
    this.maxPlanning = maxPlanning;
    this.start = start;
    this.end = end;
    this.slowestPlanning = slowestPlanning;
    this.slowestMetadata = slowestMetadata;
    this.requestsByQueue = requestsByQueue;
    this.requestCounterMap = requestCounterMap;
    this.memoryUsage = memoryUsage;
    this.totalQueryCounts = totalQueryCounts;
    this.schemaOpsCounts = schemaOpsCounts;
    this.queueCounts = queueCounts;
  }

  private String getQueriesJSONHtml() {
    final String totalCountsJs = new ConcurrentQueueWriter(this.bucketSize).generate(this.start.toEpochMilli(), this.end.toEpochMilli(), this.queueCounts, this.schemaOpsCounts, this.totalQueryCounts);
    final String maxValuesJs = new MaxTimeWriter(this.bucketSize).generate(this.start.toEpochMilli(),
        this.end.toEpochMilli(), maxPending, maxAttemps, maxQueued, maxPlanning);
    final String memoryAllocatedJs = new MemoryAllocatedWriter(this.bucketSize).generate(this.start.toEpochMilli(),
        this.end.toEpochMilli(), this.memoryUsage);
    final String requestCounter = RequestCounterWriter.generate(this.totalQueries, this.requestCounterMap);
    final String requestQueueCounter = RequestByQueueWriter.generate(this.totalQueries, this.requestsByQueue);
    final String summaryText = generateSummary();
    final String slowestMetadataQueries = SlowestMetadataRetrievalWriter.generate(this.totalQueries,
        this.slowestMetadata);
    final String slowestPlanningQueries = SlowestPlanningWriter.generate(this.totalQueries, this.slowestPlanning);
    final String maxMemoryQueries = MaxMemoryQueriesWriter.generateMaxMemoryAllocated(mostMemoryQueries);

    return "<!doctype html>\n"
        + "<html   lang=\"en\">\n"
        + "<head>\n"
        + "  <meta charset=\"utf-8\">\n"
        + "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        + "  <title>Queries.json report</title>\n"
        + "  <meta name\"description\" content=\"report for queries.json\">\n"
        + "  <meta name=\"author\" content=\"dremio\">\n"
        + "  <meta property=\"og:title\" content=\"queries.json report\">\n"
        + "  <meta property=\"og:type\" content=\"website\">\n"
        + "  <meta property=\"og:description\" content=\"plotly generated graphs for"
        + " queries.json\">\n"
        + "<style>\n"
        + jsLibraryTextProvider.getTableCSS()
        + "</style>\n"
        + "  <script>"
        + jsLibraryTextProvider.getPlotlyJsText()
        + "</script>\n"
        + "  <script>\n"
        + jsLibraryTextProvider.getCSVExportText()
        + "</script>\n"
        + "<style>\n"
        + jsLibraryTextProvider.getSortableCSSText()
        + "</style>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getSortableText()
        + "</script>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getCSVExportText()
        + "</script>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getFilterTableText()
        + "</script>\n"
        + "</head>\n<body>"
        + ""
        + "<div style=\"display: grid; column-gap: 50px;\">\n"
        + "<div style=\"grid-row:1; padding: 25px;\">"
        + summaryText
        + "</div>"
        + "<div style=\"grid-row:1;padding: 25px;\">"
        + requestQueueCounter
        + "</div>"
        + "<div style=\"grid-row:1;\">"
        + requestCounter
        + "</div>"
        + "<div style=\"grid-row:2; grid-column-start: 1; grid-column-end:"
        + " 4;padding: 50px;\">"
        + slowestMetadataQueries
        + "</div>"
        + "<div style=\"grid-row:2; grid-column-start: 1; grid-column-end:"
        + " 4;padding: 50px;\">"
        + slowestPlanningQueries
        + "</div>"
        + "<div style=\"grid-row:2; grid-column-start: 1; grid-column-end:"
        + " 4;padding: 50px;\">"
        + maxMemoryQueries
        + "</div>"
        + "</div>"
        + totalCountsJs
        + maxValuesJs
        + memoryAllocatedJs
        + "</body>";
  }

  @Override
  public String getText() {
    if (this.totalQueries == 0) {
      return "no queries found";
    } else {
      LOGGER.info(() -> this.totalQueries + " queries parsed");
    }
    return this.getQueriesJSONHtml();
  }

  private String generateSummary() {
    final StringBuilder builder = new StringBuilder();
    if (this.totalQueries == 0) {
      builder.append("<h2>Queries Summary</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }

    final Collection<Collection<HtmlTableDataColumn<String, Long>>> rows = new ArrayList<>();
    rows.add(
        asList(col("first query start"), col(Dates.format(this.start))));
    final long durationMillis = this.end.toEpochMilli() - this.start.toEpochMilli();
    final double durationSeconds = durationMillis / 1000.0;
    rows.add(asList(col("last query end"), col(Dates.format(this.end))));
    rows.add(asList(col("time span"), col(Human.getHumanDurationFromMillis(durationMillis))));
    rows.add(asList(col("total queries"), col(String.format("%,d", this.totalQueries))));
    rows.add(
        asList(
            col("average queries per second"),
            col(String.format("%.2f", (this.totalQueries / durationSeconds)))));
    var htmlBuilder = new HtmlTableBuilder();
    builder.append(
        htmlBuilder.generateTable(
            "queriesSummary", "Queries Summary", asList("name", "value"), rows));
    return builder.toString();
  }

  @Override
  public String getTitle() {
    return "Queries.json Report";
  }
}
