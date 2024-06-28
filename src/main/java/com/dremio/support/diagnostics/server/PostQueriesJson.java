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

import com.dremio.support.diagnostics.queriesjson.QueriesJsonHtmlReport;
import com.dremio.support.diagnostics.queriesjson.ReadArchive;
import com.dremio.support.diagnostics.queriesjson.filters.DateRangeQueryFilter;
import com.dremio.support.diagnostics.queriesjson.reporters.ConcurrentQueriesReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.ConcurrentQueueReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.ConcurrentSchemaOpsReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.MaxCPUQueriesReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.MaxMemoryQueriesReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.MaxTimeReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.MemoryAllocatedReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.QueryReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.RequestCounterReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.RequestsByQueueReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.SlowestMetadataQueriesReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.SlowestPlanningQueriesReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.StartFinishReporter;
import com.dremio.support.diagnostics.queriesjson.reporters.TotalQueriesReporter;
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
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

public class PostQueriesJson implements Handler {
  private static final Logger logger = Logger.getLogger(PostQueriesJson.class.getName());
  private final UsageLogger usageLogger;

  public PostQueriesJson(final UsageLogger usageLogger) {
    this.usageLogger = usageLogger;
  }

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    var functionStart = Instant.now();
    var files = ctx.uploadedFiles();
    if (files.size() != 1) {
      throw new InvalidParameterException("expected one file but had %d".formatted(files.size()));
    }
    var file = files.get(0);
    var fields = ctx.formParamMap();
    try (InputStream is = file.content()) {

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        final StreamWriterReporter reporter = new StreamWriterReporter(baos);
        var windowStr = "86400000";
        final List<String> windowArray = fields.getOrDefault("window", Arrays.asList("86400000"));
        if (windowArray.size() == 1) {
          windowStr = windowArray.get(0);
        }
        int windowRaw;
        try {
          windowRaw = Integer.parseInt(windowStr);
        } catch (NumberFormatException ex) {
          windowRaw = 86400000;
          logger.warning("unable to parse number %s due to error %s".formatted(windowStr, ex));
        }
        final int window = windowRaw;
        final ZoneId z = ZoneId.of("UTC");

        final int thisYear = ZonedDateTime.now(z).getYear();
        String startDate = "%d-01-01".formatted(thisYear - 1);
        final List<String> startDateParams =
            fields.getOrDefault("start_date", List.of("%d-01-01".formatted(thisYear - 1)));
        if (startDateParams.size() == 1) {
          startDate = startDateParams.get(0);
        }
        String startTime = "00:00";
        final List<String> startTimeParams = fields.getOrDefault("start_time", List.of("00:00"));
        if (startTimeParams.size() == 1) {
          startTime = startTimeParams.get(0);
        }

        String endDate = "%d-01-01".formatted(thisYear + 2);
        final List<String> endDateParams =
            fields.getOrDefault("end_date", List.of("%d-01-01".formatted(thisYear + 2)));
        if (endDateParams.size() == 1) {
          endDate = endDateParams.get(0);
        }

        String endTime = "00:00";
        final List<String> endTimeParams = fields.getOrDefault("end_time", List.of("00:00"));
        if (endTimeParams.size() == 1) {
          endTime = endTimeParams.get(0);
        }

        Instant start = Instant.parse(String.format("%sT%s:00.000Z", startDate, startTime));
        Instant end = Instant.parse(String.format("%sT%s:00.000Z", endDate, endTime));

        final List<String> limitParams = fields.getOrDefault("limit", List.of("5"));
        final int limit;
        if (limitParams.size() == 1) {
          var limitRaw = limitParams.get(0);
          limit = Integer.parseInt(limitRaw);
        } else {
          limit = 5;
        }
        var reporters = new ArrayList<QueryReporter>();
        final ConcurrentQueriesReporter concurrentQueriesReporter =
            new ConcurrentQueriesReporter(window);
        reporters.add(concurrentQueriesReporter);
        final ConcurrentQueueReporter concurrentQueueReporter = new ConcurrentQueueReporter(window);
        reporters.add(concurrentQueueReporter);
        final ConcurrentSchemaOpsReporter concurrentSchemaOpsReporter =
            new ConcurrentSchemaOpsReporter(window);
        reporters.add(concurrentSchemaOpsReporter);
        final MaxMemoryQueriesReporter maxMemoryQueriesReporter =
            new MaxMemoryQueriesReporter(limit);
        reporters.add(maxMemoryQueriesReporter);
        final MaxCPUQueriesReporter maxCPUQueriesReporter = new MaxCPUQueriesReporter(limit);
        reporters.add(maxCPUQueriesReporter);
        final MaxTimeReporter maxTimeReporter = new MaxTimeReporter(window);
        reporters.add(maxTimeReporter);
        final MemoryAllocatedReporter memoryAllocatedReporter = new MemoryAllocatedReporter(window);
        reporters.add(memoryAllocatedReporter);
        final RequestCounterReporter requestCounterReporter = new RequestCounterReporter();
        reporters.add(requestCounterReporter);
        final RequestsByQueueReporter requestsByQueueReporter = new RequestsByQueueReporter();
        reporters.add(requestsByQueueReporter);
        final SlowestMetadataQueriesReporter slowestMetadataQueriesReporter =
            new SlowestMetadataQueriesReporter(limit);
        reporters.add(slowestMetadataQueriesReporter);
        final SlowestPlanningQueriesReporter slowestPlanningQueriesReporter =
            new SlowestPlanningQueriesReporter(limit);
        reporters.add(slowestPlanningQueriesReporter);
        final StartFinishReporter startFinishReporter = new StartFinishReporter();
        reporters.add(startFinishReporter);
        final TotalQueriesReporter totalQueriesReporter = new TotalQueriesReporter();
        reporters.add(totalQueriesReporter);

        var filter = new DateRangeQueryFilter(start.toEpochMilli(), end.toEpochMilli());
        var archive = new ReadArchive(filter);
        var cpus = Runtime.getRuntime().availableProcessors() / 2;
        var tmpFile = Files.createTempFile("dqd", "tmp");
        try (FileOutputStream os = new FileOutputStream(tmpFile.toFile())) {
          var buff = new byte[65536];
          IOUtils.copyLarge(is, os, buff);
        }
        if (file.filename().endsWith(".tgz") || file.filename().endsWith(".tar.gz")) {
          archive.readTarGz(tmpFile.toString(), reporters, cpus);
        } else if (file.filename().endsWith(".zip")) {
          archive.readZip(tmpFile.toString(), reporters, cpus);
        } else {
          throw new RuntimeException("unknown extension for file " + file.filename().toString());
        }
        new com.dremio.support.diagnostics.queriesjson.Exec()
            .run(
                new QueriesJsonHtmlReport(
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
                    totalQueriesReporter),
                reporter);
        ctx.html(baos.toString(StandardCharsets.UTF_8));
      }
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "unexpected error", ex);
      ctx.html("<html><body>" + ex.getMessage() + "</body>");
    } finally {
      logger.info("queries.json report generated");
      var end = Instant.now();
      usageLogger.LogUsage(
          new UsageEntry(
              functionStart.getEpochSecond(), end.getEpochSecond(), "queries-json", ctx.ip()));
    }
  }
}
