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

import com.dremio.support.diagnostics.queriesjson.html.*;
import com.dremio.support.diagnostics.queriesjson.reporters.*;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import com.dremio.support.diagnostics.shared.Report;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

public class QueriesJsonHtmlReport implements Report {
  private static final Logger LOGGER = Logger.getLogger(QueriesJsonHtmlReport.class.getName());
  private final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();
  private final Collection<Query> failedQueries;
  private final long problematicQueryLimit;
  private final Instant startFilter;
  private final Instant endFilter;
  private final Instant start;
  private final Instant end;
  private final long totalQueries;
  private final Map<String, Long> requestCounterMap;
  private final Map<String, Long> requestsByQueue;
  private final Map<Long, Double> memoryUsage;
  private final Collection<Query> slowestPlanning;
  private final Collection<Query> slowestMetadata;
  private final long bucketSize;
  private final Map<Long, Long> maxPending;
  private final Map<Long, Long> maxMetadata;
  private final Map<Long, Long> maxQueued;
  private final Map<Long, Long> maxPlanning;
  private final Map<Long, Long> totalQueryCounts;
  private final Map<Long, Long> schemaOpsCounts;
  private final Map<String, Map<Long, Long>> queueCounts;
  private final Collection<SearchedFile> filesSearched;

  private final Collection<Query> mostMemoryQueries;
  private final Collection<Query> mostCpuTimeQueries;
  private final Map<Long, Long> maxPool;

  public QueriesJsonHtmlReport(
      Collection<SearchedFile> filesSearched,
      final Instant startFilter,
      final Instant endFilter,
      long bucketSize,
      final ConcurrentQueriesReporter concurrentQueriesReporter,
      final ConcurrentQueueReporter concurrentQueueReporter,
      final ConcurrentSchemaOpsReporter concurrentSchemaOpsReporter,
      final MaxMemoryQueriesReporter maxMemoryQueriesReporter,
      final MaxCPUQueriesReporter maxCpuQueriesReporter,
      final MaxTimeReporter maxTimeReporter,
      final MemoryAllocatedReporter memoryAllocatedReporter,
      final RequestCounterReporter requestCounterReporter,
      final RequestsByQueueReporter requestsByQueueReporter,
      final SlowestMetadataQueriesReporter slowestMetadataQueriesReporter,
      final SlowestPlanningQueriesReporter slowestPlanningQueriesReporter,
      final StartFinishReporter startFinishReporter,
      final TotalQueriesReporter totalQueriesReporter,
      final FailedQueriesReporter failedQueriesReporter,
      final long problematicQueryLimit) {
    this(
        filesSearched,
        startFilter,
        endFilter,
        bucketSize,
        totalQueriesReporter.getCount(),
        slowestPlanningQueriesReporter.getQueries(),
        slowestMetadataQueriesReporter.getQueries(),
        maxMemoryQueriesReporter.getQueries(),
        maxCpuQueriesReporter.getQueries(),
        requestCounterReporter.getRequestCounterMap(),
        requestsByQueueReporter.getRequestsByQueue(),
        memoryAllocatedReporter.getMemoryCounter(),
        maxTimeReporter.getPending(),
        maxTimeReporter.getMetadata(),
        maxTimeReporter.getQueued(),
        maxTimeReporter.getPlanning(),
        maxTimeReporter.getMaxPool(),
        concurrentQueriesReporter.getCounts(),
        concurrentSchemaOpsReporter.getBuckets(),
        concurrentQueueReporter.getQueueBucketCounts(),
        Instant.ofEpochMilli(startFinishReporter.getStart()),
        Instant.ofEpochMilli(startFinishReporter.getFinish()),
        failedQueriesReporter.getFailedQueries(),
        problematicQueryLimit);
  }

  public QueriesJsonHtmlReport(
      final Collection<SearchedFile> filesSearched,
      final Instant startFilter,
      final Instant endFilter,
      final long bucketSize,
      final long totalQueries,
      final Collection<Query> slowestPlanning,
      final Collection<Query> slowestMetadata,
      final Collection<Query> mostMemoryQueries,
      final Collection<Query> mostCpuTimeQueries,
      final Map<String, Long> requestCounterMap,
      final Map<String, Long> requestsByQueue,
      final Map<Long, Double> memoryUsage,
      final Map<Long, Long> maxPending,
      final Map<Long, Long> maxMetadata,
      final Map<Long, Long> maxQueued,
      final Map<Long, Long> maxPlanning,
      final Map<Long, Long> maxPool,
      final Map<Long, Long> totalQueryCounts,
      final Map<Long, Long> schemaOpsCounts,
      final Map<String, Map<Long, Long>> queueCounts,
      final Instant start,
      final Instant end,
      final Collection<Query> failedQueries,
      final long problematicQueryLimit) {
    this.filesSearched = filesSearched;
    this.startFilter = startFilter;
    this.endFilter = endFilter;
    this.bucketSize = bucketSize;
    this.totalQueries = totalQueries;
    this.mostMemoryQueries = mostMemoryQueries;
    this.mostCpuTimeQueries = mostCpuTimeQueries;
    this.maxPending = maxPending;
    this.maxMetadata = maxMetadata;
    this.maxQueued = maxQueued;
    this.maxPlanning = maxPlanning;
    this.maxPool = maxPool;
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
    this.failedQueries = failedQueries;
    this.problematicQueryLimit = problematicQueryLimit;
  }

  private String getQueriesJSONHtml() {
    long durationMillis = this.end.toEpochMilli() - this.start.toEpochMilli();
    if (durationMillis < this.bucketSize) {
      return """
 <!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="utf-8">
 <meta name="viewport" content="width=device-width, initial-scale=1"/>
 <title>Queries.json report</title>
 <meta name"description" content="report for queries.json">
 <meta name="author" content="dremio">
 <meta property="og:title" content="queries.json report">
 <meta property="og:type" content="website">
 <meta property="og:description" content="plotly generated graphs for queries.json">
 <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.classless.min.css" />
 </head>
 <body>
     <main>
      <h3>bucket size is too large</h3>
      <p>Selected bucket size of %d milliseconds is bigger than the range examined %d of milliseconds. Try again with a bucket size of 1 second</p>
     </main>
 </body>
 </html>
"""
          .formatted(this.bucketSize, durationMillis);
    }
    final String totalCountsJs =
        new ConcurrentQueueWriter(this.bucketSize)
            .generate(
                this.start.toEpochMilli(),
                this.end.toEpochMilli(),
                this.queueCounts,
                this.schemaOpsCounts,
                this.totalQueryCounts);
    final String maxValuesJs =
        new MaxTimeWriter(this.bucketSize)
            .generate(
                this.start.toEpochMilli(),
                this.end.toEpochMilli(),
                maxPending,
                maxMetadata,
                maxQueued,
                maxPlanning,
                maxPool);
    final String memoryAllocatedJs =
        new MemoryAllocatedWriter(this.bucketSize)
            .generate(this.start.toEpochMilli(), this.end.toEpochMilli(), this.memoryUsage);
    final String requestCounter =
        RequestCounterWriter.generate(this.totalQueries, this.requestCounterMap);
    final String requestQueueCounter =
        RequestByQueueWriter.generate(this.totalQueries, this.requestsByQueue);
    final String summaryText = generateSummary();
    final String slowestMetadataQueries =
        SlowestMetadataRetrievalWriter.generate(this.totalQueries, this.slowestMetadata);
    final String slowestPlanningQueries =
        SlowestPlanningWriter.generate(this.totalQueries, this.slowestPlanning);
    final String maxMemoryQueries =
        MaxMemoryQueriesWriter.generateMaxMemoryAllocated(mostMemoryQueries);
    final String maxCpuTime = MaxCPUTimeWriter.generate(mostCpuTimeQueries);
    final String failedQueries =
        FailedQueriesWriter.generateTable(this.failedQueries, this.problematicQueryLimit);
    return """
 <!DOCTYPE html>
 <html lang="en">
 <head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Queries.json report</title>
  <meta name"description" content="report for queries.json">
  <meta name="author" content="dremio">
  <meta property="og:title" content="queries.json report">
  <meta property="og:type" content="website">
  <meta property="og:description" content="plotly generated graphs for queries.json">
  <style>
     html {
      scroll-behavior: smooth;
    }
     table {
     table-layout:fixed; width: 100%%;
     }
     .summary-page {
         display: grid;
         grid-template-columns: repeat(3, 1fr);
         grid-gap: 10px;
         grid-auto-rows: minmax(100px, auto);
     }
     .content-page {
         display: grid;
         grid-template-columns: repeat(2, 1fr);
         grid-gap: 10px;
         grid-auto-rows: minmax(100px, auto);
     }
     .tooltip-pr {
       overflow: hidden;
       white-space: nowrap;
       text-overflow: ellipsis;
     }

     .tooltip-pr .tooltiptext-pr {
       color: black;
       hyphens: auto;
     }

     .tooltip-pr:hover {
       cursor: pointer;
       white-space: initial;
       transition: height 0.2s ease-in-out;
     }

     /* Style the navbar */
     #navbar {
       overflow: hidden;
       background-color: #333;
       z-index: 289;
     }

     /* Navbar links */
     #navbar a {
       float: left;
       display: block;
       color: #f2f2f2;
       text-align: center;
       padding: 14px;
       text-decoration: none;
     }
       #navbar .active-link {
         color: white;
         background-color: green;
       }

     /* Page content */
     .content {
       //padding-top: 50px;
     }

     /* The sticky class is added to the navbar with JS when it reaches its scroll position */
     .sticky {
       position: fixed;
       top: 0;
       width: 100%%;
     }

     /* Add some top padding to the page content to prevent sudden quick movement (as the navigation bar gets a new position at the top of the page (position:fixed and top:0) */
     .sticky + .content {
       padding-top: 100px;
     }
 </style>
  <style>
    %s
  </style>
  <script>
    %s
  </script>
  <script>
   %s
  </script>
  <style>
    %s
  </style>
  <script>
    %s
  </script>
  <script>
    %s
  </script>

 </head>
 <body>
 <div id="navbar">
   <div style="float: left;">
   <h3 style="color: white" >queries.json report</h3>
   </div>
   <div style="float:right;">
   <a class="nav-link" href="#summary-section">Summary</a>
   <a class="nav-link" href="#outliers-section">Outliers</a>
   <a class="nav-link" href="#usage-section">Usage</a>
   <a class="nav-link" href="#failures-section">Failures</a>
   </div>
 </div>
 <main class="content">
 <section id="summary-section">
 <h3>Summary</h3>
 <div class="summary-page">
  <div>%s</div>
  <div>%s</div>
  <div>%s</div>
 </div>
 </section>
 <section id="outliers-section">
 <h3>Outliers</h3>
 <div class="content-page">
  <div>%s</div>
  <div>%s</div>
  <div>%s</div>
  <div>%s</div>
 </div>
 </section>
 <section id="usage-section">
 <h3>Usage</h3>
 %s
 %s
 %s
 </section>
 <section id="failures-section">
 <h3>Failures</h3>
 %s
 </section>
 </main>
 <script>
   // When the user scrolls the page, execute myFunction
   window.onscroll = function() {stickNav()};

   // Get the navbar
   var navbar = document.getElementById("navbar");

   // Get the offset position of the navbar
   var sticky = navbar.offsetTop;

   // Add the sticky class to the navbar when you reach its scroll position. Remove "sticky" when you leave the scroll position
   function stickNav() {
     if (window.pageYOffset >= sticky) {
       navbar.classList.add("sticky")
     } else {
       navbar.classList.remove("sticky");
     }
   }
 </script>
<script>
    const sections = document.querySelectorAll('section');
    const links = document.querySelectorAll('a.nav-link');

    window.addEventListener('scroll', () => {
        let scrollPosition = window.scrollY + 80;
        sections.forEach(section => {
            if (scrollPosition >= section.offsetTop) {
                links.forEach(link => {
                    link.classList.remove('active-link');
                    if (section.getAttribute('id') === link.getAttribute('href').substring(1)) {
                        link.classList.add('active-link');
                    }
                });
            }
        });
    });
  </script>
 </body>
"""
        .formatted(
            jsLibraryTextProvider.getTableCSS(),
            jsLibraryTextProvider.getPlotlyJsText(),
            jsLibraryTextProvider.getCSVExportText(),
            jsLibraryTextProvider.getSortableCSSText(),
            jsLibraryTextProvider.getSortableText(),
            jsLibraryTextProvider.getFilterTableText(),
            summaryText,
            requestCounter,
            requestQueueCounter,
            slowestMetadataQueries,
            slowestPlanningQueries,
            maxCpuTime,
            maxMemoryQueries,
            totalCountsJs,
            maxValuesJs,
            memoryAllocatedJs,
            failedQueries);
  }

  @Override
  public String getText() {
    if (this.totalQueries == 0) {
      var sb =
          new StringBuilder(
              """
<!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="utf-8">
 <meta name="viewport" content="width=device-width, initial-scale=1"/>
 <title>Queries.json report</title>
 <meta name"description" content="report for queries.json">
 <meta name="author" content="dremio">
 <meta property="og:title" content="queries.json report">
 <meta property="og:type" content="website">
 <meta property="og:description" content="plotly generated graphs for queries.json">
 <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.classless.min.css" />
</head>
<body>
<main>

<h2>no queries found</h2>
<h3>filters appled</h3>
<table>
  <thead>
    <tr>
      <th>filter name</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>start filter</td>
      <td>%s</td>
    </tr>
    <tr>
      <td>end filter</td>
      <td>%s</td>
    </tr>
  </tbody>
</table>
"""
                  .formatted(startFilter, endFilter));
      sb.append("<h3>files searched</h3>");
      sb.append(
          "<table><thead><tr><th>file</th><th>queries filtered by date</th></tr></thead><tbody>");
      for (SearchedFile s : filesSearched) {
        sb.append("<tr><td>");
        sb.append(s.name());
        sb.append("</td><td>");
        sb.append(s.filtered());
        sb.append("</td></tr>");
      }
      sb.append("</tbody></table>");
      sb.append("</main></body></html>");
      return sb.toString();
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
    rows.add(asList(col("first query start"), col(Dates.format(this.start))));
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
