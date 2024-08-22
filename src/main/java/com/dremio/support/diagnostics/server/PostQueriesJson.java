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
package com.dremio.support.diagnostics.server;

import com.dremio.support.diagnostics.queriesjson.QueriesJsonFileParser;
import com.dremio.support.diagnostics.queriesjson.QueriesJsonHtmlReport;
import com.dremio.support.diagnostics.queriesjson.ReadArchive;
import com.dremio.support.diagnostics.queriesjson.SearchedFile;
import com.dremio.support.diagnostics.queriesjson.filters.DateRangeQueryFilter;
import com.dremio.support.diagnostics.queriesjson.reporters.*;
import com.dremio.support.diagnostics.shared.StreamWriterReporter;
import com.dremio.support.diagnostics.shared.UsageEntry;
import com.dremio.support.diagnostics.shared.UsageLogger;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.jetbrains.annotations.NotNull;

/**
 * end point for queries.json command, takes several parameters and handles file uploads
 */
public class PostQueriesJson implements Handler {

  /**
   * logger instance
   */
  private static final Logger logger = Logger.getLogger(PostQueriesJson.class.getName());

  /**
   * logs the generation of reports
   */
  private final UsageLogger usageLogger;

  /**
   * PostQueriesJson handles the upload of queries.json files
   *
   * @param usageLogger logs the generation of reports
   */
  public PostQueriesJson(final UsageLogger usageLogger) {
    this.usageLogger = usageLogger;
  }

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    // start time for measuring how long it takes this method to process
    var functionStart = Instant.now();
    var files = ctx.uploadedFiles();
    if (files.size() != 1) {
      throw new InvalidParameterException("expected one file but had %d".formatted(files.size()));
    }
    // read the first file uploaded, we don't support more (or less) than this.
    var file = files.get(0);
    // retrieve the configuration parameters for the analysis.
    var fields = ctx.formParamMap();
    try (final InputStream uploadedFileStream = file.content()) {
      // copy report to memory
      try (final ByteArrayOutputStream reportOutputStream = new ByteArrayOutputStream()) {
        final StreamWriterReporter reporter = new StreamWriterReporter(reportOutputStream);
        final List<String> windowArray = fields.getOrDefault("window", Arrays.asList("86400000"));
        // window size
        final String windowStr;
        if (windowArray.size() == 1) {
          windowStr = windowArray.get(0);
        } else {
          // no options passed use the default for safety reasons.
          windowStr = "86400000";
        }
        final Integer window;
        if (NumberUtils.isParsable(windowStr)) {
          window = NumberUtils.toInt(windowStr);
        } else {
          window = 86400000;
          // NOTE: we should probably be failing this instead
          logger.warning(
              "unable to parse window size '%s' as integer, falling back to default window size: %s"
                  .formatted(windowStr, window));
        }
        // always assume UTC for everything
        final ZoneId z = ZoneId.of("UTC");
        // retrieve the current year
        final int thisYear = ZonedDateTime.now(z).getYear();
        // set the default start date as last year. Not sure why we do this, should probably skip
        // this defaults
        String startDate = "%d-01-01".formatted(thisYear - 1);
        // set the start date based on the parameter
        final List<String> startDateParams =
            fields.getOrDefault("start_date", List.of("%d-01-01".formatted(thisYear - 1)));
        if (startDateParams.size() == 1) {
          startDate = startDateParams.get(0);
        }
        // set the time to midnight UTC, this is just to make it easier to reason about
        String startTime = "00:00";
        final List<String> startTimeParams = fields.getOrDefault("start_time", List.of("00:00"));
        if (startTimeParams.size() == 1) {
          startTime = startTimeParams.get(0);
        }

        // default end date as 2 years in the future, again this is probably uncessary and we should
        // just require this values
        String endDate = "%d-01-01".formatted(thisYear + 2);
        final List<String> endDateParams =
            fields.getOrDefault("end_date", List.of("%d-01-01".formatted(thisYear + 2)));
        if (endDateParams.size() == 1) {
          endDate = endDateParams.get(0);
        }

        // set the time to midnight UTC, this is just to make it easier to reason about
        String endTime = "00:00";
        final List<String> endTimeParams = fields.getOrDefault("end_time", List.of("00:00"));
        if (endTimeParams.size() == 1) {
          endTime = endTimeParams.get(0);
        }

        // get the start time
        Instant start = Instant.parse(String.format("%sT%s:00.000Z", startDate, startTime));
        // get the end time
        Instant end = Instant.parse(String.format("%sT%s:00.000Z", endDate, endTime));

        // get the limit with a default of 5
        final List<String> limitParams = fields.getOrDefault("limit", List.of("5"));
        final int limit;
        if (limitParams.size() == 1) {
          var limitRaw = limitParams.get(0);
          limit = Integer.parseInt(limitRaw);
        } else {
          // again I'm not sure why i'm not just failing the thing, this should be an exception
          limit = 5;
        }
        final List<QueryReporter> reporters = new ArrayList<>();
        // provides the concurrent queries metrics, this is basically a time series
        // based on start and end time of query, so that some queries may show up in several time
        // windows as they are still running in both
        final ConcurrentQueriesReporter concurrentQueriesReporter =
            new ConcurrentQueriesReporter(window);
        reporters.add(concurrentQueriesReporter);

        // As above, based on start and end time of a query, and long running queries may span many
        // time
        // windows. This has a time series per queue
        final ConcurrentQueueReporter concurrentQueueReporter = new ConcurrentQueueReporter(window);
        reporters.add(concurrentQueueReporter);
        // As above, based on start and end time of a query, and long running queries may span many
        // time
        // windows. This has a time series of schema ops (DROP, CREATE, ALTER, REFRESH)
        final ConcurrentSchemaOpsReporter concurrentSchemaOpsReporter =
            new ConcurrentSchemaOpsReporter(window);
        reporters.add(concurrentSchemaOpsReporter);
        // provides top <limit> queries by memory usage
        final MaxMemoryQueriesReporter maxMemoryQueriesReporter =
            new MaxMemoryQueriesReporter(limit);
        reporters.add(maxMemoryQueriesReporter);
        // provides top <limit> queries by cpu time
        final MaxCPUQueriesReporter maxCPUQueriesReporter = new MaxCPUQueriesReporter(limit);
        reporters.add(maxCPUQueriesReporter);
        // provides top <limit> queries by cpu time (start and end of the query)
        final MaxTimeReporter maxTimeReporter = new MaxTimeReporter(window);
        reporters.add(maxTimeReporter);
        // As above, based on start and end time of a query, and long running queries may span many
        // time
        // windows. This has a time series of memory usage, queries lasting longer than a second
        // have their usage divided by window size and the memory usage is applied evenly to all
        // buckets
        final MemoryAllocatedReporter memoryAllocatedReporter = new MemoryAllocatedReporter(window);
        reporters.add(memoryAllocatedReporter);
        // Just summarizes requests by outcome across the entire dataset
        final RequestCounterReporter requestCounterReporter = new RequestCounterReporter();
        reporters.add(requestCounterReporter);
        // totals up requests by outcome
        final RequestsByQueueReporter requestsByQueueReporter = new RequestsByQueueReporter();
        reporters.add(requestsByQueueReporter);
        // finds the top <limit> slowest metadata refresh queries to find outliers
        final SlowestMetadataQueriesReporter slowestMetadataQueriesReporter =
            new SlowestMetadataQueriesReporter(limit);
        reporters.add(slowestMetadataQueriesReporter);
        // finds the top <limit> slowest planning queries to find outliers
        final SlowestPlanningQueriesReporter slowestPlanningQueriesReporter =
            new SlowestPlanningQueriesReporter(limit);
        reporters.add(slowestPlanningQueriesReporter);
        // finds the first query start time and last query end time
        final StartFinishReporter startFinishReporter = new StartFinishReporter();
        reporters.add(startFinishReporter);
        // total queries count
        final TotalQueriesReporter totalQueriesReporter = new TotalQueriesReporter();
        reporters.add(totalQueriesReporter);
        // total error count
        final FailedQueriesReporter failedQueriesReporter = new FailedQueriesReporter(limit);
        reporters.add(failedQueriesReporter);
        // apply the range filter
        final DateRangeQueryFilter filter =
            new DateRangeQueryFilter(start.toEpochMilli(), end.toEpochMilli());
        // pass the filter to the archive
        final ReadArchive archive = new ReadArchive(filter);
        // allocate half the available cpus for threading
        final Integer cpus = Runtime.getRuntime().availableProcessors() / 2;
        final Path tmpFile = Files.createTempFile("dqd", "tmp");
        try (final FileOutputStream uploadTmpFileStream = new FileOutputStream(tmpFile.toFile())) {
          // copy the array to an ouput files 65k at a time
          var buff = new byte[65536];
          IOUtils.copyLarge(uploadedFileStream, uploadTmpFileStream, buff);
        }
        // setup the file search list
        final List<SearchedFile> filesSearched = new ArrayList<SearchedFile>();
        if (file.filename().endsWith(".tgz") || file.filename().endsWith(".tar.gz")) {
          filesSearched.addAll(
              archive.readTarGz(tmpFile.toString(), reporters, cpus).stream().toList());
        } else if (file.filename().endsWith(".tar.xz")) {
          filesSearched.addAll(
              archive.readTarXz(tmpFile.toString(), reporters, cpus).stream().toList());
        } else if (file.filename().endsWith(".tar.bzip2")) {
          filesSearched.addAll(
              archive.readTarBzip2(tmpFile.toString(), reporters, cpus).stream().toList());
        } else if (file.filename().endsWith(".tar")) {
          filesSearched.addAll(
              archive.readTar(tmpFile.toString(), reporters, cpus).stream().toList());
        } else if (file.filename().endsWith(".zip")) {
          filesSearched.addAll(
              archive.readZip(tmpFile.toString(), reporters, cpus).stream().toList());
        } else if (file.filename().endsWith(".gz")) {
          filesSearched.add(archive.parseGzip(tmpFile.toString(), tmpFile, reporters));
        } else if (file.filename().endsWith(".bzip2")) {
          filesSearched.add(archive.parseBzip2(tmpFile.toString(), reporters));
        } else if (file.filename().endsWith(".json")) {
          try (final InputStream newInputStream = Files.newInputStream(tmpFile)) {
            filesSearched.add(
                QueriesJsonFileParser.parseFile(
                    tmpFile.toString(),
                    newInputStream,
                    reporters,
                    new DateRangeQueryFilter(start.toEpochMilli(), end.toEpochMilli())));
          }
        } else {
          throw new RuntimeException(
              "unknown extension for file "
                  + file.filename()
                  + " only supported extensions are .tar, .tar.gz, .tgz,"
                  + " tar.xz, tar.bzip2, .bzip2, .gz, .zip and .json");
        }
        // go ahead and generate the html report
        new com.dremio.support.diagnostics.queriesjson.Exec()
            .run(
                new QueriesJsonHtmlReport(
                    filesSearched,
                    start,
                    end,
                    window,
                    concurrentQueriesReporter,
                    concurrentQueueReporter,
                    concurrentSchemaOpsReporter,
                    maxMemoryQueriesReporter,
                    maxCPUQueriesReporter,
                    maxTimeReporter,
                    memoryAllocatedReporter,
                    requestCounterReporter,
                    requestsByQueueReporter,
                    slowestMetadataQueriesReporter,
                    slowestPlanningQueriesReporter,
                    startFinishReporter,
                    totalQueriesReporter,
                    failedQueriesReporter,
                    limit),
                reporter);
        // send the report byte around to the user
        ctx.html(reportOutputStream.toString(StandardCharsets.UTF_8));
      }
    } catch (Exception ex) {
      // catch all for error messages
      logger.log(Level.SEVERE, "unexpected error", ex);
      ctx.html("<html><body>" + ex.getMessage() + "</body>");
    } finally {
      logger.info("queries.json report generated");
      var end = Instant.now();
      // log report generation
      usageLogger.LogUsage(
          new UsageEntry(
              functionStart.getEpochSecond(), end.getEpochSecond(), "queries-json", ctx.ip()));
    }
  }
}
