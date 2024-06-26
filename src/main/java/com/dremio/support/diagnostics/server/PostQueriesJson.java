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

import static com.dremio.support.diagnostics.shared.zip.ArchiveDetection.isArchive;

import com.dremio.support.diagnostics.queriesjson.QueriesJsonHtmlReport;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.StreamWriterReporter;
import com.dremio.support.diagnostics.shared.UsageEntry;
import com.dremio.support.diagnostics.shared.UsageLogger;
import com.dremio.support.diagnostics.shared.zip.Extraction;
import com.dremio.support.diagnostics.shared.zip.UnzipperImpl;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    final List<Extraction> extractions = new ArrayList<>();
    try (InputStream is = file.content()) {
      final List<PathAndStream> pathAndStreams = new ArrayList<>();
      PathAndStream inputFile = new PathAndStream(Paths.get(file.filename()), is);
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        if (isArchive(file.filename())) {
          extractions.addAll(
              new UnzipperImpl()
                  .unzipAllFiles(
                      inputFile,
                      x -> {
                        final var base = Paths.get(x).getFileName().toString();
                        logger.info(() -> "is a json file %b".formatted(base.contains(".json")));
                        logger.info(
                            () -> "starts with queries %b".formatted(base.startsWith("queries")));
                        return base.contains(".json") && base.startsWith("queries");
                      }));
          for (final Extraction e : extractions) {
            pathAndStreams.addAll(e.getPathAndStreams());
          }
          logger.info(
              () -> String.format("zip file turning into %d query files", pathAndStreams.size()));
        } else {
          pathAndStreams.add(inputFile);
        }
        final StreamWriterReporter reporter = new StreamWriterReporter(baos);
        var fallbackBucketSecondsString = "3600";
        final List<String> fallbackBucketSecondsStringArray =
            fields.getOrDefault("fallback_bucket_seconds", Arrays.asList("3600"));
        if (fallbackBucketSecondsStringArray.size() == 1) {
          fallbackBucketSecondsString = fallbackBucketSecondsStringArray.get(0);
        }
        int fallbackBucketSecondsRaw;
        try {
          fallbackBucketSecondsRaw = Integer.parseInt(fallbackBucketSecondsString);
        } catch (NumberFormatException ex) {
          fallbackBucketSecondsRaw = 3600;
          logger.warning(
              "unable to parse number %s due to error %s"
                  .formatted(fallbackBucketSecondsString, ex));
        }
        final int fallbackBucketSeconds = fallbackBucketSecondsRaw;
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
        new com.dremio.support.diagnostics.queriesjson.Exec()
            .run(
                pathAndStreams,
                (streams) ->
                    new QueriesJsonHtmlReport(
                        limit, streams, complexityLimit, fallbackBucketSeconds, start, end),
                reporter);
        ctx.html(baos.toString(StandardCharsets.UTF_8));
      }
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "unexpected error", ex);
      ctx.html("<html><body>" + ex.getMessage() + "</body>");
    } finally {
      for (Extraction e : extractions) {
        e.close();
      }
      logger.info("queries.json report generated");
      var end = Instant.now();
      usageLogger.LogUsage(
          new UsageEntry(
              functionStart.getEpochSecond(), end.getEpochSecond(), "queries-json", ctx.ip()));
    }
  }
}
