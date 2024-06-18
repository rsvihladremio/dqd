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
package com.dremio.support.diagnostics.simple;

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;
import static com.dremio.support.diagnostics.shared.Human.*;

import com.dremio.support.diagnostics.profilejson.CoreOperatorType;
import com.dremio.support.diagnostics.profilejson.QueryState;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelationshipParser;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.FindingsReport;
import com.dremio.support.diagnostics.repro.ArgSetup;
import com.dremio.support.diagnostics.shared.*;
import com.dremio.support.diagnostics.shared.dto.profilejson.*;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine;

public class ProfileJSONSimplified {
  public record Summary(
      String dremioVersion,
      long startEpochMillis,
      long endEpochMillis,
      int totalPhases,
      long totalOperators,
      String user,
      String queryPhase,
      Collection<String> findings,
      Collection<OperatorRow> operatorRows) {}

  public record OperatorRow(
      String hostName,
      long batches,
      long records,
      long sizeBytes,
      long operatorId,
      long phaseId,
      long threadId,
      long processNanos,
      long setupNanos,
      long waitNanos,
      long totalDurationNanos,
      long peakMemoryAllocatedBytes,
      CoreOperatorType coreOperatorType,
      String hostname) {

    public String name() {
      return String.format(
          "%s %02d-%02d-%02d", coreOperatorType(), phaseId(), threadId(), operatorId());
    }

    public double processSeconds() {
      if (processNanos == 0) {
        return 0;
      }
      return processNanos / 1000000000.0;
    }

    public double waitSeconds() {
      if (waitNanos == 0) {
        return 0;
      }
      return waitNanos / 1000000000.0;
    }

    public double setupSeconds() {
      if (setupNanos == 0) {
        return 0.0;
      }
      return setupNanos / 1000000000.0;
    }

    public double totalDurationSeconds() {
      if (totalDurationNanos == 0) {
        return 0.0;
      }
      return totalDurationNanos / 1000000000.0;
    }
  }

  public static class Summarize {

    public Summary singleProfile(ProfileJSON parsedProfileJSON) {
      String dremioVersion = "unknown";
      String user = "unknown user";
      String queryPhase = "UNKNOWN PHASE";
      long start = 0;
      long end = 0;
      int totalPhases = 0;
      List<OperatorRow> rows = new ArrayList<>();
      if (parsedProfileJSON == null) {
        return new Summary(
            dremioVersion, start, end, totalPhases, 0, user, queryPhase, new ArrayList<>(), rows);
      }
      final Collection<PlanRelation> planRelations =
          new PlanRelationshipParser().getPlanRelations(parsedProfileJSON);

      var findings = FindingsReport.searchForFindings(parsedProfileJSON, planRelations);
      if (parsedProfileJSON.getFragmentProfile() != null) {
        start = parsedProfileJSON.getStart();
        end = parsedProfileJSON.getEnd();
        if (parsedProfileJSON.getUser() != null && !parsedProfileJSON.getUser().isEmpty()) {
          user = parsedProfileJSON.getUser();
        }
        if (parsedProfileJSON.getState() < QueryState.values().length) {
          queryPhase = QueryState.values()[parsedProfileJSON.getState()].name();
        }
        if (parsedProfileJSON.getDremioVersion() != null
            && !parsedProfileJSON.getDremioVersion().isEmpty()) {
          dremioVersion = parsedProfileJSON.getDremioVersion();
        }
        for (final FragmentProfile fragmentProfile : parsedProfileJSON.getFragmentProfile()) {
          if (fragmentProfile != null) {
            final int phaseId = fragmentProfile.getMajorFragmentId();
            totalPhases++;
            for (final MinorFragmentProfile minorFragmentProfile :
                fragmentProfile.getMinorFragmentProfile()) {
              if (minorFragmentProfile != null
                  && minorFragmentProfile.getOperatorProfile() != null) {
                final long threadId = minorFragmentProfile.getMinorFragmentId();
                String hostName = "";
                if (minorFragmentProfile.getEndpoint() != null
                    && minorFragmentProfile.getEndpoint().getAddress() != null) {
                  hostName = minorFragmentProfile.getEndpoint().getAddress();
                }
                for (final OperatorProfile operatorProfile :
                    minorFragmentProfile.getOperatorProfile()) {
                  long batches = 0;
                  long records = 0;
                  long size = 0;
                  if (operatorProfile.getInputProfile() != null) {
                    for (final InputProfile inputProfile : operatorProfile.getInputProfile()) {
                      batches += inputProfile.getBatches();
                      records += inputProfile.getRecords();
                      size += inputProfile.getSize();
                    }
                  }
                  final int operatorTypeId = operatorProfile.getOperatorType();
                  final long operatorId = operatorProfile.getOperatorId();
                  final long processNanos = operatorProfile.getProcessNanos();
                  final long setupNanos = operatorProfile.getSetupNanos();
                  final long waitNanos = operatorProfile.getWaitNanos();
                  final long totalDurationNanos = processNanos + setupNanos + waitNanos;
                  final long peakLocalMemoryAllocated =
                      operatorProfile.getPeakLocalMemoryAllocated();
                  final CoreOperatorType[] coreOperatorTypes = CoreOperatorType.values();
                  CoreOperatorType operatorType = null;
                  if ((long) operatorTypeId < coreOperatorTypes.length) {
                    operatorType = coreOperatorTypes[operatorTypeId];
                  }
                  final OperatorRow row =
                      new OperatorRow(
                          hostName,
                          batches,
                          records,
                          size,
                          operatorId,
                          phaseId,
                          threadId,
                          processNanos,
                          setupNanos,
                          waitNanos,
                          totalDurationNanos,
                          peakLocalMemoryAllocated,
                          operatorType,
                          hostName);
                  rows.add(row);
                }
              }
            }
          }
        }
      }
      return new Summary(
          dremioVersion,
          start,
          end,
          totalPhases,
          rows.size(),
          user,
          queryPhase,
          findings,
          rows.stream()
              .sorted((x1, x2) -> Long.compare(x2.totalDurationNanos(), x1.totalDurationNanos()))
              .toList());
    }

    public SummaryCompare compareProfiles(ProfileJSON profile1, ProfileJSON profile2) {
      Summary summary1 = singleProfile(profile1);
      Summary summary2 = singleProfile(profile2);
      return new SummaryCompare(summary1, summary2);
    }
  }

  public record SummaryCompare(Summary summary1, Summary summary2) {}

  public static class ProfileHTTPEndpoint implements Handler {

    private static final Logger logger = Logger.getLogger(ProfileHTTPEndpoint.class.getName());
    private final UsageLogger usageLogger;

    public ProfileHTTPEndpoint(UsageLogger usageLogger) {
      this.usageLogger = usageLogger;
    }

    @Override
    public void handle(@NotNull Context ctx) throws Exception {
      var start = Instant.now();
      try (InputStream is = ctx.uploadedFiles().get(0).content()) {
        ProfileProvider profileProvider =
            ArgSetup.getProfileProvider(
                new PathAndStream(Paths.get(ctx.uploadedFiles().get(0).filename()), is));
        ProfileJSON p = profileProvider.getProfile();
        final Summary summary = new Summarize().singleProfile(p);
        final int unlimitedRows = -1;
        final String text = new HTMLReport().singleProfile(summary, unlimitedRows);
        ctx.html(text);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "error reading uploaded file", e);
        ctx.html("<html><body>" + e.getMessage() + "</body>");
      } finally {
        logger.info("profile analysis report generated");
        var end = Instant.now();
        usageLogger.LogUsage(
            new UsageEntry(
                start.getEpochSecond(), end.getEpochSecond(), "profile-json-simple", ctx.ip()));
      }
    }
  }

  public static class HTMLReport {
    public String profileCompare(
        final Summary summary1, final Summary summary2, Integer limitOperatorRows) {
      String fragment1 = summaryFragment(summary1, 1, limitOperatorRows);
      String fragment2 = summaryFragment(summary2, 2, limitOperatorRows);

      return createHTMLPage(
          "Profile JSON Comparison",
          """
              <div style="float: left">
                <h4>Profile 1</h4>
                %s
              </div>
              <div style="float: right">
                <h4>Profile 2</h4>
                %s
              </div>
              """
              .formatted(fragment1, fragment2));
    }

    public String summaryFragment(final Summary summary, int id, int limitOperatorRows) {
      var duration =
          Human.getHumanDurationFromMillis(summary.endEpochMillis() - summary.startEpochMillis());
      var formatter = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.of("UTC"));
      var startTimeHuman = formatter.format(Instant.ofEpochMilli(summary.startEpochMillis()));
      var endTimeHuman = formatter.format(Instant.ofEpochMilli(summary.endEpochMillis()));
      var builder = new HtmlTableBuilder();
      String caption = "Query Summary";
      if (limitOperatorRows > 0) {
        caption = "Query Summary (showing the %d slowest rows)".formatted(limitOperatorRows);
      }
      final HtmlTableDataColumn<String, Number> startTimeCol = HtmlTableDataColumn.col("start");
      final HtmlTableDataColumn<String, Number> startTimeHumanCol =
          HtmlTableDataColumn.col(startTimeHuman);
      final List<HtmlTableDataColumn<String, Number>> startTimeRow =
          Arrays.asList(startTimeCol, startTimeHumanCol);
      final HtmlTableDataColumn<String, Number> endTimeCol = HtmlTableDataColumn.col("end");
      final HtmlTableDataColumn<String, Number> endTimeHumanCol =
          HtmlTableDataColumn.col(endTimeHuman);
      final List<HtmlTableDataColumn<String, Number>> endTimeRow =
          Arrays.asList(endTimeCol, endTimeHumanCol);
      final List<HtmlTableDataColumn<String, Number>> durationRow =
          Arrays.asList(HtmlTableDataColumn.col("duration"), HtmlTableDataColumn.col(duration));
      final List<HtmlTableDataColumn<String, Number>> phaseRow =
          Arrays.asList(
              HtmlTableDataColumn.col("phase"), HtmlTableDataColumn.col(summary.queryPhase()));
      final List<HtmlTableDataColumn<String, Number>> totalPhasesRow =
          Arrays.asList(
              HtmlTableDataColumn.col("total phases"),
              HtmlTableDataColumn.col(String.valueOf(summary.totalPhases())));
      final List<HtmlTableDataColumn<String, Number>> totalOperatorsRow =
          Arrays.asList(
              HtmlTableDataColumn.col("total operators"),
              HtmlTableDataColumn.col(String.valueOf(summary.totalOperators())));
      final List<HtmlTableDataColumn<String, Number>> dremioVersionRow =
          Arrays.asList(
              HtmlTableDataColumn.col("dremio version"),
              HtmlTableDataColumn.col(summary.dremioVersion()));
      final List<HtmlTableDataColumn<String, Number>> userRow =
          Arrays.asList(HtmlTableDataColumn.col("user"), HtmlTableDataColumn.col(summary.user()));
      final String topLineSummaryHTMLTable =
          builder.generateTable(
              "querySummaryTable" + id,
              caption,
              Arrays.asList("name", "value"),
              Arrays.asList(
                  startTimeRow,
                  endTimeRow,
                  durationRow,
                  phaseRow,
                  totalPhasesRow,
                  totalOperatorsRow,
                  dremioVersionRow,
                  userRow));
      final Collection<Collection<HtmlTableDataColumn<String, Number>>> operatorTableRows =
          new ArrayList<>();
      var operatorRowsStream = summary.operatorRows().stream();
      if (limitOperatorRows > 0) {
        operatorRowsStream = operatorRowsStream.limit(limitOperatorRows);
      }
      operatorRowsStream
          .map(
              x ->
                  Arrays.<HtmlTableDataColumn<String, Number>>asList(
                      HtmlTableDataColumn.col(x.name()),
                      col(getHumanDurationFromNanos(x.processNanos()), x.processNanos()),
                      col(getHumanDurationFromNanos(x.waitNanos()), x.waitNanos()),
                      col(getHumanDurationFromNanos(x.setupNanos()), x.setupNanos()),
                      col(
                          getHumanDurationFromNanos(x.totalDurationNanos()),
                          x.totalDurationNanos()),
                      col(getHumanBytes1024(x.sizeBytes()), x.sizeBytes()),
                      col(getHumanNumber(x.batches()), x.batches()),
                      col(getHumanNumber(x.records()), x.records()),
                      col(
                          getHumanNumber(x.records() / x.totalDurationSeconds()),
                          ((double) Math.round((x.records() * 100.0) / x.totalDurationSeconds())
                              / 100.0)),
                      col(
                          getHumanBytes1024(x.peakMemoryAllocatedBytes()),
                          x.peakMemoryAllocatedBytes()),
                      HtmlTableDataColumn.col(x.hostname())))
          .forEach(operatorTableRows::add);

      final String operatorHTMLTable =
          builder.generateTable(
              "operatorsTable" + id,
              "Operators",
              Arrays.asList(
                  "Name",
                  "Process",
                  "Wait",
                  "Setup",
                  "Total",
                  "Size Processed",
                  "Batches",
                  "Records",
                  "Records/Sec",
                  "Peak RAM Allocated",
                  "node"),
              operatorTableRows);
      Collection<Collection<HtmlTableDataColumn<String, Integer>>> findings = new ArrayList<>();
      int counter = 0;
      for (var finding : summary.findings()) {
        counter++;
        findings.add(
            Arrays.asList(col(String.valueOf(counter), counter), HtmlTableDataColumn.col(finding)));
      }
      final String findingsHTMLTable =
          builder.generateTable(
              "findingsTable" + id, "Findings", Arrays.asList("num", "desc"), findings);
      return """
          <div>%s</div>
          <div>%s</div>
          <div>%s</div>
          """
          .formatted(topLineSummaryHTMLTable, findingsHTMLTable, operatorHTMLTable);
    }

    private String createHTMLPage(String title, String content) {
      var provider = new JsLibraryTextProvider();
      return """
          <!DOCTYPE html>
          <html lang="en">
            <head>
              <meta charset="UTF-8">
              <meta name="viewport" content="width=device-width, initial-scale=1.0">
              <title>Profile Summary</title>
              <script>%s</script>
              <script>%s</script>
              <style>%s</style>
              <style>%s</style>
              <script>%s</script>
            </head>
            <body>
              <main>
                <h1>%s</h1>
                %s
              </main>
            </body>
          </html>
           """
          .formatted(
              provider.getCSVExportText(),
              provider.getSortableText(),
              provider.getSortableCSSText(),
              provider.getTableCSS(),
              provider.getFilterTableText(),
              title,
              content);
    }

    public String singleProfile(final Summary summary, int limitOperatorRows) {
      return createHTMLPage("Profile JSON Summary", summaryFragment(summary, 0, limitOperatorRows));
    }
  }

  public record CliArgs(File path, File comparePath, int limitOperatorRows) {}

  @CommandLine.Command(
      name = "summarize-profile-json",
      mixinStandardHelpOptions = true,
      description = "A simplified profile-json command that only includes a brief summary")
  public static class Cli implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "The file whose checksum to calculate.")
    private File profile;

    @CommandLine.Option(
        names = {"-c", "--compare"},
        description = "A second profile to compare against")
    private File profileToCompare;

    @CommandLine.Option(
        names = {"-l", "--limit-operator-rows"},
        defaultValue = "-1",
        description = "optional limit on the rows in the operators table(s)")
    private Integer limitOperatorRows;

    public String execute(CliArgs args) {
      if (args.comparePath() == null) {
        try (final InputStream stream = Files.newInputStream(args.path().toPath())) {
          var profile =
              ArgSetup.getProfileProvider(new PathAndStream(args.path().toPath(), stream));
          final Summarize summarize = new Summarize();
          try {
            final Summary summary = summarize.singleProfile(profile.getProfile());
            var reporter = new ProfileJSONSimplified.HTMLReport();
            return reporter.singleProfile(summary, args.limitOperatorRows);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        try (final InputStream stream = Files.newInputStream(args.path().toPath())) {
          try (final InputStream compareStream =
              Files.newInputStream(args.comparePath().toPath())) {
            var profile =
                ArgSetup.getProfileProvider(new PathAndStream(args.path().toPath(), stream));
            var compareProfile =
                ArgSetup.getProfileProvider(
                    new PathAndStream(args.comparePath().toPath(), compareStream));
            final Summarize summarize = new Summarize();
            try {
              SummaryCompare compare =
                  summarize.compareProfiles(profile.getProfile(), compareProfile.getProfile());

              var reporter = new ProfileJSONSimplified.HTMLReport();

              return reporter.profileCompare(
                  compare.summary1(), compare.summary2(), args.limitOperatorRows());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public Integer call() throws Exception {
      final var args = new CliArgs(profile, profileToCompare, limitOperatorRows);
      String outputText = execute(args);
      System.out.println(outputText);
      return 0;
    }
  }
}
