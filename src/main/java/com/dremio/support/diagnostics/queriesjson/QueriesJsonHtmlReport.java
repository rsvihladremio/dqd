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
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;

import com.dremio.support.diagnostics.shared.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueriesJsonHtmlReport implements Report {
  private static final Logger LOGGER = Logger.getLogger(QueriesJsonHtmlReport.class.getName());
  private final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();
  private final long complexityLimit;
  private final long secondsFallback;
  private final Stream<Query> queries;
  private final DateTimeFormatter formatter =
      DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.of("UTC"));
  private Instant start;
  private Instant end;
  private int limit;

  public QueriesJsonHtmlReport(
      final int limit,
      final Stream<Query> queries,
      final long complexityLimit,
      final long secondsFallback,
      final Instant start,
      final Instant end) {
    this.limit = limit;
    this.queries = queries;
    this.complexityLimit = complexityLimit;
    this.secondsFallback = secondsFallback;
    this.start = start;
    this.end = end;
  }

  private String getQueriesJSONHtml(
      final List<Query> sortedQueries, final long duration, final long start, final long finish) {
    long estimatedBuckets = duration / 1000;
    long complexity = sortedQueries.size() * estimatedBuckets;
    LOGGER.info(() -> String.format("estimated complexity score %d", complexity));
    final long bucketSize;
    final String fallbackText;
    if (complexity > complexityLimit) {
      bucketSize = 1000L * this.secondsFallback;
      fallbackText =
          "<h2>NOTE: Too many queries fallback to %s bucket size</h2>"
              .formatted(Human.getHumanDurationFromMillis(bucketSize));
      LOGGER.warning("using reduced resolution due to complexity limit being too high");
      // throw new TooMuchComplexityException(complexity, complexityLimit);
    } else {
      fallbackText = "";
      bucketSize = 1000L;
    }

    final Graph bucketGraph;
    bucketGraph = new BucketGraph(start, finish, bucketSize);
    final int numberThreads = Math.max(2, Runtime.getRuntime().availableProcessors() - 1);
    final ExecutorService threadPool = Executors.newFixedThreadPool(numberThreads);
    try {
      final Future<String> totalCountFuture =
          threadPool.submit(() -> generateConcurrentQueriesJS(bucketGraph, sortedQueries));
      final Future<String> maxValuesFuture =
          threadPool.submit(() -> generateMaxValuesJS(bucketGraph, sortedQueries));
      final Future<String> memoryAllocatedFuture =
          threadPool.submit(() -> generateMemoryAllocatedJS(bucketGraph, sortedQueries));
      final String totalCountsJs = totalCountFuture.get();
      final String maxValuesJs = maxValuesFuture.get();
      final String memoryAllocatedJs = memoryAllocatedFuture.get();
      final String requestCounter = generateRequestCounter(sortedQueries);
      final String requestQueueCounter = generateRequestByQueue(sortedQueries);
      final String requestEngineCounter = generateRequestByEngine(sortedQueries);
      final String summaryText = generateSummary(sortedQueries);
      final String slowestMetadataQueries =
          generateSlowestMetaDataRetrieveQueriesTable(sortedQueries);
      final String slowestPlanningQueries = generateSlowestPlanningQueriesTable(sortedQueries);
      final String maxMemoryQueries = generateMaxMemoryAllocated(sortedQueries);

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
          + fallbackText
          + "<div style=\"display: grid; column-gap: 50px;\">\n"
          + "<div style=\"grid-row:1; padding: 25px;\">"
          + summaryText
          + "</div>"
          + "<div style=\"grid-row:1;padding: 25px;\">"
          + requestQueueCounter
          + "</div>"
          + "<div style=\"grid-row:1;\">"
          + requestCounter
          + requestEngineCounter
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
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      threadPool.shutdown();
    }
  }

  @Override
  public String getText() {
    long startEpoch = this.start.toEpochMilli();
    long endEpoch = this.end.toEpochMilli();
    List<Query> sortedQueries =
        this.queries
            .filter(x -> x.getStart() >= startEpoch)
            .filter(x -> x.getStart() <= endEpoch)
            .sorted(Comparator.comparing(Query::getStart))
            .collect(Collectors.toList());
    if (sortedQueries.isEmpty()) {
      return "no queries found";
    } else {
      LOGGER.info(() -> sortedQueries.size() + " queries parsed");
    }
    long start = sortedQueries.stream().map(Query::getStart).min(Long::compareTo).get();
    long finish = sortedQueries.stream().map(Query::getFinish).max(Long::compareTo).get();
    long duration = finish - start;
    return this.getQueriesJSONHtml(sortedQueries, duration, start, finish);
  }

  private String generateSummary(final Collection<Query> sortedQueries) {
    final StringBuilder builder = new StringBuilder();
    if (sortedQueries.isEmpty()) {
      builder.append("<h2>Queries Summary</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }

    final Collection<Collection<HtmlTableDataColumn<String, Long>>> rows = new ArrayList<>();
    final long startTime =
        sortedQueries.stream()
            .map(Query::getStart)
            .reduce(
                Long.MAX_VALUE,
                (left, right) -> {
                  if (left < right) {
                    return left;
                  } else {
                    return right;
                  }
                });
    rows.add(
        asList(col("first query start"), col(formatter.format(Instant.ofEpochMilli(startTime)))));
    final long endTime =
        sortedQueries.stream()
            .map(Query::getFinish)
            .reduce(
                0L,
                (left, right) -> {
                  if (left > right) {
                    return left;
                  } else {
                    return right;
                  }
                });
    final long durationMillis = endTime - startTime;
    final double durationSeconds = durationMillis / 1000.0;
    rows.add(asList(col("last query end"), col(formatter.format(Instant.ofEpochMilli(endTime)))));
    rows.add(asList(col("time span"), col(Human.getHumanDurationFromMillis(endTime - startTime))));
    rows.add(asList(col("total queries"), col(String.format("%,d", sortedQueries.size()))));
    rows.add(
        asList(
            col("average queries per second"),
            col(String.format("%.2f", (sortedQueries.size() / durationSeconds)))));
    final Query maxQueued =
        sortedQueries.stream()
            .reduce(
                new Query(),
                (left, right) -> {
                  if (left.getQueuedTime() > right.getQueuedTime()) {
                    return left;
                  } else {
                    return right;
                  }
                });
    rows.add(
        asList(
            col("max time in queue"),
            col(
                String.format(
                    "%s at %s",
                    Human.getHumanDurationFromMillis(maxQueued.getQueuedTime()),
                    formatter.format(Instant.ofEpochMilli(maxQueued.getStart()))))));
    final Query maxPending =
        sortedQueries.stream()
            .reduce(
                new Query(),
                (left, right) -> {
                  if (left.getPendingTime() > right.getPendingTime()) {
                    return left;
                  } else {
                    return right;
                  }
                });
    rows.add(
        asList(
            col("max pending time"),
            col(
                String.format(
                    "%s at %s",
                    Human.getHumanDurationFromMillis(maxPending.getPendingTime()),
                    formatter.format(Instant.ofEpochMilli(maxPending.getStart()))))));
    final Query maxPoolWait =
        sortedQueries.stream()
            .reduce(
                new Query(),
                (left, right) -> {
                  if (left.getPoolWaitTime() > right.getPoolWaitTime()) {
                    return left;
                  } else {
                    return right;
                  }
                });
    rows.add(
        asList(
            col("max pool wait time"),
            col(
                String.format(
                    "%s at %s",
                    Human.getHumanDurationFromMillis(maxPoolWait.getPoolWaitTime()),
                    formatter.format(Instant.ofEpochMilli(maxPoolWait.getStart()))))));
    var htmlBuilder = new HtmlTableBuilder();
    builder.append(
        htmlBuilder.generateTable(
            "queriesSummary", "Queries Summary", asList("name", "value"), rows));
    return builder.toString();
  }

  private String generateSlowestMetaDataRetrieveQueriesTable(
      final Collection<Query> sortedQueries) {
    final StringBuilder builder = new StringBuilder();
    if (sortedQueries.isEmpty()) {
      builder.append("<h2>Slowest Metadata Retrieval</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    var htmlBuilder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    sortedQueries.stream()
        .sorted(Comparator.comparingLong(Query::getNormalizedMetadataRetrieval).reversed())
        .limit(this.limit)
        .forEach(
            x ->
                rows.add(
                    asList(
                        col(x.getQueryId()),
                        col(formatter.format(Instant.ofEpochMilli(x.getStart())), x.getStart()),
                        col(
                            Human.getHumanDurationFromMillis(x.getFinish() - x.getStart()),
                            x.getFinish() - x.getStart()),
                        col(
                            Human.getHumanDurationFromMillis(x.getNormalizedMetadataRetrieval()),
                            x.getNormalizedMetadataRetrieval()),
                        col(x.getQueryText()))));
    builder.append(
        htmlBuilder.generateTable(
            "metatdataRetrievalQueriesTable",
            "Slowest Metadata Retrieval",
            asList("query id", "start", "query duration", "metadata retrieval time", "query"),
            rows));
    return builder.toString();
  }

  private String generateMaxMemoryAllocated(final Collection<Query> sortedQueries) {
    final StringBuilder builder = new StringBuilder();
    if (sortedQueries.isEmpty()) {
      builder.append("<h2>Max Memory Allocated</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    var htmlBuilder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    sortedQueries.stream()
        .sorted(Comparator.comparingLong(Query::getMemoryAllocated).reversed())
        .limit(this.limit)
        .forEach(
            x ->
                rows.add(
                    asList(
                        col(x.getQueryId()),
                        col(formatter.format(Instant.ofEpochMilli(x.getStart())), x.getStart()),
                        col(
                            Human.getHumanDurationFromMillis(x.getFinish() - x.getStart()),
                            x.getFinish() - x.getStart()),
                        col(
                            Human.getHumanBytes1024(x.getMemoryAllocated()),
                            x.getMemoryAllocated()),
                        col(x.getQueryText()))));

    builder.append(
        htmlBuilder.generateTable(
            "maxMemoryAllocatedTable",
            "Max Memory Allocated",
            asList("query id", "start", "query duration", "mem", "query"),
            rows));
    return builder.toString();
  }

  private String generateSlowestPlanningQueriesTable(final Collection<Query> sortedQueries) {
    final StringBuilder builder = new StringBuilder();
    if (sortedQueries.isEmpty()) {
      builder.append("<h2>Slowest Planning</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    var htmlBuilder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    sortedQueries.stream()
        .sorted(Comparator.comparingLong(Query::getPlanningTime).reversed())
        .limit(this.limit)
        .forEach(
            x ->
                rows.add(
                    asList(
                        col(x.getQueryId()),
                        col(formatter.format(Instant.ofEpochMilli(x.getStart())), x.getStart()),
                        col(
                            Human.getHumanDurationFromMillis(x.getFinish() - x.getStart()),
                            x.getFinish() - x.getStart()),
                        col(
                            Human.getHumanDurationFromMillis(x.getPlanningTime()),
                            x.getPlanningTime()),
                        col(x.getQueryText()))));
    builder.append(
        htmlBuilder.generateTable(
            "slowestPlaningQueriesTable",
            "Slowest Planning",
            asList("query id", "start", "query duration", "planning time", "query"),
            rows));
    return builder.toString();
  }

  private String generateRequestByQueue(final Collection<Query> sortedQueries) {
    if (sortedQueries.isEmpty()) {
      return "<h2>Total Queries by Queue</h2><p>No Queries Found</p>";
    }
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    final long totalCount = sortedQueries.size();
    return builder.generateTable(
        "totalQueriesByQueue",
        "Total Queries by Queue",
        asList("Queue", "count", "%"),
        sortedQueries.stream()
            .collect(Collectors.groupingBy(Query::getQueueName))
            .entrySet()
            .stream()
            .sorted(
                (l, r) -> {
                  Integer leftCount = (Integer) l.getValue().size();
                  Integer rightCount = (Integer) r.getValue().size();
                  return rightCount.compareTo(leftCount);
                })
            .map(
                x ->
                    Arrays.<HtmlTableDataColumn<String, Number>>asList(
                        col(x.getKey()),
                        col(String.format("%,d", x.getValue().size()), x.getValue().size()),
                        col(
                            String.format("%.2f", 100.0 * x.getValue().size() / totalCount),
                            x.getValue().size())))
            .collect(Collectors.toList()));
  }

  private String generateRequestByEngine(final Collection<Query> sortedQueries) {
    if (sortedQueries.isEmpty()) {
      return "<h2>Total Queries by Engine</h2><p>No Queries Found</p>";
    }
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    final long totalCount = sortedQueries.size();
    return builder.generateTable(
        "totalQueriesByEngine",
        "Total Queries by Engine",
        asList("Queue", "count", "%"),
        sortedQueries.stream()
            .collect(Collectors.groupingBy(Query::getEngineName))
            .entrySet()
            .stream()
            .sorted(
                (l, r) -> {
                  final Integer leftCount = (Integer) l.getValue().size();
                  final Integer rightCount = (Integer) r.getValue().size();
                  return rightCount.compareTo(leftCount);
                })
            .map(
                x ->
                    Arrays.<HtmlTableDataColumn<String, Number>>asList(
                        col(x.getKey()),
                        col(String.format("%,d", x.getValue().size()), x.getValue().size()),
                        col(
                            String.format("%.2f", 100.0 * x.getValue().size() / totalCount),
                            x.getValue().size())))
            .collect(Collectors.toList()));
  }

  private String generateRequestCounter(final Collection<Query> sortedQueries) {
    StringBuilder builder = new StringBuilder();
    if (sortedQueries.isEmpty()) {
      builder.append("<h2>Query Outcomes</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    final Map<String, Long> requestCounterMap = new LinkedHashMap<>();
    for (final Query query : sortedQueries) {
      final String outcome = query.getOutcome();
      if (requestCounterMap.containsKey(outcome)) {
        final Long total = requestCounterMap.get(outcome);
        requestCounterMap.put(outcome, total + 1L);
      } else {
        requestCounterMap.put(outcome, 1L);
      }
    }
    var tableBuilder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    final int totalCount = sortedQueries.size();
    requestCounterMap.entrySet().stream()
        .map(x -> asList(x.getKey(), x.getValue(), (100.0 * x.getValue()) / totalCount))
        .forEach(
            x -> {
              rows.add(
                  asList(
                      col(String.valueOf(x.get(0))),
                      col(String.format("%,d", (Long) x.get(1)), (Long) x.get(1)),
                      col(String.format("%.2f", ((Double) x.get(2))), (Double) x.get(2))));
            });
    builder.append(
        tableBuilder.generateTable(
            "queryOutcomes", "Query Outcomes", asList("outcome", "count", "%"), rows));
    return builder.toString();
  }

  private String writeTraceHtml(String traceId, String title, DataPoints dp) {
    StringBuilder xStr = new StringBuilder();
    StringBuilder yStr = new StringBuilder();
    long[] x = dp.getTimestamps();
    long[] y = dp.getValues();
    for (int i = 0; i < x.length; i++) {
      xStr.append("\"");
      xStr.append(Instant.ofEpochMilli(x[i]));
      xStr.append("\"");
      yStr.append(y[i]);
      if (i != x.length - 1) {
        xStr.append(",");
        yStr.append(",");
      }
    }
    return "var "
        + traceId
        + " = {x:["
        + xStr
        + "],y:["
        + yStr
        + "],mode: 'lines',"
        + "xaxis: 'x',"
        + "yaxis: 'y',"
        + "type: 'scatter',"
        + "name: '"
        + title
        + "'};";
  }

  private String writePlotHtml(String title, String plotId, String[] traceIds, String... traces) {
    StringBuilder builder = new StringBuilder();
    builder.append("\n<div id='");
    builder.append(plotId);
    builder.append("' />");
    builder.append("<script type=\"text/javascript\">\n");
    builder.append("var ");
    builder.append(plotId);
    builder.append(" = document.getElementById('");
    builder.append(plotId);
    builder.append("');\n");
    for (String trace : traces) {
      builder.append(trace);
      builder.append("\n");
    }
    builder.append("Plotly.newPlot(");
    builder.append(plotId);
    builder.append(",");
    builder.append("[");
    builder.append(String.join(", ", traceIds));
    builder.append("],");
    builder.append("{title: '");
    builder.append(title);
    builder.append("',");
    builder.append("height: 450,");
    builder.append("width: 1280});\n");
    builder.append("</script>");
    return builder.toString();
  }

  private String generateMemoryAllocatedJS(Graph bucketGraph, List<Query> sortedQueries) {
    LOGGER.fine("memory allocation started");
    ZonedDateTime methodStart = ZonedDateTime.now(UTC);
    DataPoints memoryAllocated =
        bucketGraph.getFilteredBuckets(
            sortedQueries,
            BucketGraph::noOp,
            (q, values, index) -> {
              long oldValue = values[index];
              long queryDuration = q.getFinish() - q.getStart();
              long oneSecond = 1000;
              if (queryDuration > oneSecond) {
                double queryDurationSeconds = queryDuration / 1000.0;
                double allocatedMemory = (q.getMemoryAllocated() / queryDurationSeconds) / 1048576L;
                values[index] = Math.round(allocatedMemory + oldValue);
              } else {
                values[index] = Math.round((double) (q.getMemoryAllocated() + oldValue) / 1048576L);
              }
            });

    String memoryAllocatedId = "memoryAllocated";
    String memoryAllocatedTrace =
        writeTraceHtml(memoryAllocatedId, "bytes allocated", memoryAllocated);
    String memTimeTitle =
        "Queries.json ESTIMATED memory allocated per %s in MB (1048576 bytes)"
            .formatted(Human.getHumanDurationFromMillis(bucketGraph.getBucketSizeMillis()));
    String memTimePlot =
        writePlotHtml(
            memTimeTitle,
            "memory_allocated",
            new String[] {memoryAllocatedId},
            memoryAllocatedTrace);
    LOGGER.fine(
        () -> {
          ZonedDateTime methodEnd = ZonedDateTime.now(UTC);
          long diff = ChronoUnit.SECONDS.between(methodStart, methodEnd);
          return "memory allocation done: " + diff + " seconds";
        });
    return memTimePlot;
  }

  String generateMaxValuesJS(Graph bucketGraph, List<Query> sortedQueries) {
    LOGGER.info("max values started");
    final ExecutorService threadPool = Executors.newFixedThreadPool(3);
    try {
      ZonedDateTime methodStart = ZonedDateTime.now(UTC);
      Future<String> maxPendingFuture =
          threadPool.submit(
              () -> {
                DataPoints maxPending =
                    bucketGraph.getFilteredBuckets(
                        sortedQueries,
                        BucketGraph::noOp,
                        (q, values, index) -> {
                          long oldValue = values[index];
                          values[index] = Math.max(q.getPendingTime(), oldValue);
                        });
                // set values down to one second
                long[] d = maxPending.getValues();
                for (int i = 0; i < d.length; i++) {
                  if (d[i] > 0) {
                    d[i] = d[i] / 1000;
                  }
                }
                maxPending.setValues(d);
                return writeTraceHtml("maxPending", "max seconds pending time", maxPending);
              });
      Future<String> maxAttemptFuture =
          threadPool.submit(
              () -> {
                DataPoints maxAttempt =
                    bucketGraph.getFilteredBuckets(
                        sortedQueries,
                        BucketGraph::noOp,
                        (query, values, index) -> {
                          long oldValue = values[index];
                          values[index] = Math.max(query.getAttemptCount(), oldValue);
                        });
                return writeTraceHtml("maxAttempts", "max attempts", maxAttempt);
              });
      Future<String> maxQueuedFuture =
          threadPool.submit(
              () -> {
                DataPoints maxQueued =
                    bucketGraph.getFilteredBuckets(
                        sortedQueries,
                        BucketGraph::noOp,
                        (query, values, index) -> {
                          long oldValue = values[index];
                          values[index] = Math.max(query.getQueuedTime(), oldValue);
                        });
                // set values down to one second
                long[] d = maxQueued.getValues();
                for (int i = 0; i < d.length; i++) {
                  if (d[i] > 0) {
                    d[i] = d[i] / 1000;
                  }
                }
                maxQueued.setValues(d);
                return writeTraceHtml("maxQueued", "max seconds queued", maxQueued);
              });
      Future<String> maxPlanning =
          threadPool.submit(
              () -> {
                DataPoints maxPlan =
                    bucketGraph.getFilteredBuckets(
                        sortedQueries,
                        BucketGraph::noOp,
                        (query, values, index) -> {
                          long oldValue = values[index];
                          values[index] = Math.max(query.getPlanningTime(), oldValue);
                        });
                // set values down to one second
                long[] d = maxPlan.getValues();
                for (int i = 0; i < d.length; i++) {
                  if (d[i] > 0) {
                    d[i] = d[i] / 1000;
                  }
                }
                maxPlan.setValues(d);
                return writeTraceHtml("maxPlanning", "max seconds in planning", maxPlan);
              });
      String js;
      try {
        js =
            writePlotHtml(
                "Queries.json max values per %s"
                    .formatted(Human.getHumanDurationFromMillis(bucketGraph.getBucketSizeMillis())),
                "max_values",
                new String[] {"maxPending", "maxAttempts", "maxQueued", "maxPlanning"},
                maxPendingFuture.get(),
                maxAttemptFuture.get(),
                maxQueuedFuture.get(),
                maxPlanning.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      LOGGER.fine(
          () -> {
            ZonedDateTime methodEnd = ZonedDateTime.now(UTC);
            long diff = ChronoUnit.SECONDS.between(methodStart, methodEnd);
            return "max values done: " + diff + " seconds";
          });
      return js;
    } finally {
      threadPool.shutdown();
    }
  }

  String escape(String name) {
    return name.replace(" ", "_")
        .replace("[", "_")
        .replace("]", "_")
        .replace("|", "_")
        .replace("%", "_")
        .replace("^", "_")
        .replace("=", "_")
        .replace("+", "_")
        .replace("#", "_")
        .replace("&", "_")
        .replace("@", "_")
        .replace("*", "_")
        .replace("$", "_")
        .replace("\'", "_")
        .replace("\"", "_")
        .replace("\\", "_")
        .replace("/", "_")
        .replace(">", "_")
        .replace("<", "_")
        .replace("!", "_")
        .replace("?", "_")
        .replace(",", "_")
        .replace(".", "_")
        .replace(":", "_")
        .replace(";", "_")
        .replace("-", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("]", "_")
        .replace("[", "_");
  }

  String generateConcurrentQueriesJS(Graph bucketGraph, List<Query> sortedQueries) {
    ZonedDateTime methodStart = ZonedDateTime.now(UTC);
    final List<Future<String>> futures = new ArrayList<>();
    final Function<String, String> getEngineTraceId =
        (engineName) -> String.format("engineName%s", escape(engineName));
    final int threads = Math.max(2, Runtime.getRuntime().availableProcessors() - 1);
    final ExecutorService threadPool = Executors.newFixedThreadPool(threads);
    try {
      Future<String> totalCountsFuture =
          threadPool.submit(
              () -> {
                DataPoints totalCounts =
                    bucketGraph.getFilteredBuckets(
                        sortedQueries, BucketGraph::noOp, new CountAggregator());
                return writeTraceHtml("allQueries", "all queries", totalCounts);
              });
      futures.add(totalCountsFuture);

      final Set<String> queryEngines = new HashSet<>();
      sortedQueries.stream().forEach(x -> queryEngines.add(x.getEngineName()));
      for (String engineName : queryEngines) {
        futures.add(
            threadPool.submit(
                () -> {
                  DataPoints totalCounts =
                      bucketGraph.getFilteredBuckets(
                          sortedQueries,
                          x -> engineName.equals(x.getEngineName()),
                          new CountAggregator());
                  String traceIdName = getEngineTraceId.apply(engineName);
                  return writeTraceHtml(traceIdName, "by engine " + engineName, totalCounts);
                }));
      }
      final Function<String, String> getQueueTraceId =
          (queueName) -> String.format("queueName%s", escape(queueName));
      final Set<String> queues = new HashSet<>();
      sortedQueries.stream().forEach(x -> queues.add(x.getQueueName()));

      for (String queue : queues) {
        futures.add(
            threadPool.submit(
                () -> {
                  DataPoints totalCounts =
                      bucketGraph.getFilteredBuckets(
                          sortedQueries,
                          x -> queue.equals(x.getQueueName()),
                          new CountAggregator());
                  String traceIdName = getQueueTraceId.apply(queue);
                  return writeTraceHtml(traceIdName, "by queue " + queue, totalCounts);
                }));
      }
      Future<String> schemaFuture =
          threadPool.submit(
              () -> {
                DataPoints schemaQueries =
                    bucketGraph.getFilteredBuckets(
                        sortedQueries,
                        (q) ->
                            q.getQueryText() != null
                                && (q.getQueryText().startsWith("DROP")
                                    || q.getQueryText().startsWith("CREATE")
                                    || q.getQueryText().startsWith("REFRESH")
                                    || q.getQueryText().startsWith("ALTER")),
                        new CountAggregator());
                return writeTraceHtml(
                    "schemaQueries", "refresh, drop, alter, create queries", schemaQueries);
              });
      futures.add(schemaFuture);
      String js;
      List<String> traceIds = new ArrayList<>();
      traceIds.add("allQueries");
      traceIds.add("schemaQueries");
      for (String engineName : queryEngines) {
        traceIds.add(getEngineTraceId.apply(engineName));
      }
      for (String queue : queues) {
        traceIds.add(getQueueTraceId.apply(queue));
      }
      js =
          writePlotHtml(
              "Queries.json queries active per %s"
                  .formatted(Human.getHumanDurationFromMillis(bucketGraph.getBucketSizeMillis())),
              "total_counts",
              traceIds.toArray(new String[0]),
              futures.stream()
                  .map(
                      x -> {
                        try {
                          return x.get();
                        } catch (InterruptedException | ExecutionException e) {
                          throw new RuntimeException(e);
                        }
                      })
                  .toArray(String[]::new));
      LOGGER.fine(
          () -> {
            ZonedDateTime methodEnd = ZonedDateTime.now(UTC);
            long diff = ChronoUnit.SECONDS.between(methodStart, methodEnd);
            return "total count done: " + diff + " seconds";
          });
      return js;
    } finally {
      threadPool.shutdown();
    }
  }

  @Override
  public String getTitle() {
    return "Queries.json Report";
  }
}
