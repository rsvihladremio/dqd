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
package com.dremio.support.diagnostics.profilejson.singlefile.reports;

import com.dremio.support.diagnostics.profilejson.QueryState;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelationshipParser;
import com.dremio.support.diagnostics.profilejson.singlefile.PhaseBlockStats;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.BlockReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.BlockReport.MostBlockedReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.FindingsReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.MemoryUsed;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.MemoryUsedPerNode;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.OperatorRecordDetail;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.OperatorsRecordsScannedReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.OperatorsRecordsScannedReport.OperatorRecordsScannedReportResult;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.RowEstimateReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.RowEstimateReport.RowEstimateDetail;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.StateTimingsReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.StateTimingsReport.StateTiming;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.Report;
import com.dremio.support.diagnostics.shared.dto.profilejson.*;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

public class ProfileSummaryConsoleReport implements Report {

  private static final Logger LOGGER =
      Logger.getLogger(ProfileSummaryConsoleReport.class.getName());
  private final ProfileJSON parsed;
  private final boolean showPlanDetails;
  private final PlanRelationshipParser planParser = new PlanRelationshipParser();
  private final String operatorTemplate =
      "%s. records(%,d) batches(%,d) memory used(%s) run time(%s) p/sec(%,.2f) name(%s)\n"
          + "\t|\n"
          + "\t-> condition: %s\n\n";
  private final String fileName;

  public ProfileSummaryConsoleReport(
      final String fileName, final ProfileJSON parsed, final boolean showPlanDetails) {
    if (parsed == null) {
      throw new InvalidParameterException(
          "we cannot have a null profile, this is clearly a critical error, report this and make a"
              + " Support Tools jira");
    }
    this.fileName = fileName;
    this.parsed = parsed;
    this.showPlanDetails = showPlanDetails;
  }

  @Override
  public String getText() throws IOException {
    final StringBuilder builder = new StringBuilder();
    builder.append("\n\n");
    builder.append("sql query\n");
    builder.append("---------\n");
    builder.append(this.parsed.getQuery());
    builder.append("\n\n");
    builder.append("profile details\n");
    builder.append("---------------\n");
    addRow(builder, "file name       ", this.fileName);
    addRow(builder, "dremio version  ", this.parsed.getDremioVersion());
    addRow(builder, "start time (UTC)", Instant.ofEpochMilli(this.parsed.getStart()));
    addRow(builder, "end time (UTC)  ", Instant.ofEpochMilli(this.parsed.getEnd()));
    addRow(
        builder,
        "duration        ",
        Human.getHumanDurationFromMillis(this.parsed.getEnd() - this.parsed.getStart()));
    addRow(builder, "user            ", this.parsed.getUser());
    addRow(builder, "query state     ", QueryState.values()[this.parsed.getState()]);
    final String queue;
    if (this.parsed.getResourceSchedulingProfile() != null) {
      queue = this.parsed.getResourceSchedulingProfile().getQueueName();
    } else {
      queue = "N/A";
    }
    addRow(builder, "queue           ", queue);
    addRow(
        builder,
        "command pool    ",
        Human.getHumanDurationFromMillis(this.parsed.getCommandPoolWaitMillis()));
    final List<PlanRelation> planRelations;
    final String jsonPlan = this.parsed.getJsonPlan();
    if (jsonPlan != null) {
      planRelations = planParser.getPlanRelations(this.parsed);
    } else {
      planRelations = new ArrayList<>();
    }
    LOGGER.fine(() -> String.format("%d plans phases found%n", planRelations.size()));
    final long phaseCount =
        planRelations.stream()
            .map(x -> Iterables.get(Splitter.on('-').split(x.getName()), 0))
            .distinct()
            .count();
    addRow(builder, "total phases    ", phaseCount);
    final int nodeCount;
    if (this.parsed.getNodeProfile() != null) {
      nodeCount = this.parsed.getNodeProfile().size();
    } else {
      nodeCount = 0;
    }
    addRow(builder, "executors used ", nodeCount);
    final Foreman foreman = this.parsed.getForeman();
    if (foreman != null) {
      addRow(builder, "coord address  ", foreman.getAddress());
      addRow(builder, "end time (UTC)  ", Instant.ofEpochMilli(this.parsed.getEnd()));
      addRow(builder, "coord cores    ", foreman.getAvailableCores());
      addRow(
          builder,
          "coord direct mem",
          Human.getHumanBytes1024((long) foreman.getMaxDirectMemory()));
    }
    builder.append("\n");
    builder.append("state timings\n");
    builder.append("-------------\n");
    final Collection<StateTiming> stateTimings = StateTimingsReport.getStateTimings(parsed);
    for (final StateTiming entry : stateTimings) {
      builder.append(
          String.format(
              "* %s - %s (%.2f%%)\n",
              StringUtils.rightPad(entry.getName(), 20),
              Human.getHumanDurationFromMillis(entry.getDurationMillis()),
              entry.getPercentageOfQueryTime()));
    }

    final OperatorRecordsScannedReportResult recordsReport =
        OperatorsRecordsScannedReport.generateRecordReport(parsed, planRelations);
    if (recordsReport != null) {
      if (recordsReport.getTop10RecordsScanned() != null) {
        builder.append("\n");
        builder.append("most records scanned by operator\n");
        builder.append("---------------------------\n");
        final List<OperatorRecordDetail> top10RecordsScanned =
            recordsReport.getTop10RecordsScanned();
        int i = 0;
        for (final OperatorRecordDetail detail : top10RecordsScanned) {
          i++;
          builder.append(
              String.format(
                  operatorTemplate,
                  String.format("%1$3s", i),
                  detail.getRecords(),
                  detail.getBatches(),
                  Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                  Human.getHumanDurationFromNanos(detail.getRunTimeNanos()),
                  (detail.getRecords() * 100.0) / (detail.getRunTimeNanos() / 1000000000.0),
                  detail.getName(),
                  detail.getCondition()));
        }
      }
      if (recordsReport.getTop10SlowestScanned() != null) {
        builder.append("\n");
        builder.append("slowest operators with num records scanned\n");
        builder.append("---------------------------\n");
        final List<OperatorRecordDetail> top10Slowest = recordsReport.getTop10SlowestScanned();
        int i = 0;
        for (final OperatorRecordDetail detail : top10Slowest) {
          i++;
          builder.append(
              String.format(
                  operatorTemplate,
                  String.format("%1$3s", i),
                  detail.getRecords(),
                  detail.getBatches(),
                  Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                  Human.getHumanDurationFromNanos(detail.getRunTimeNanos()),
                  (detail.getRecords() * 100.0) / (detail.getRunTimeNanos() / 1000000000.0),
                  detail.getName(),
                  detail.getCondition()));
        }
      }
      if (recordsReport.getTop10SlowestScannedRate() != null) {

        builder.append("\n");
        builder.append("slowest operators by scan rate\n");
        builder.append("---------------------------\n");
        final List<OperatorRecordDetail> top10SlowestByRate =
            recordsReport.getTop10SlowestScannedRate();
        int i = 0;
        for (final OperatorRecordDetail detail : top10SlowestByRate) {
          i++;
          builder.append(
              String.format(
                  operatorTemplate,
                  String.format("%1$3s", i),
                  detail.getRecords(),
                  detail.getBatches(),
                  Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                  Human.getHumanDurationFromNanos(detail.getRunTimeNanos()),
                  (detail.getRecords() * 100.0) / (detail.getRunTimeNanos() / 1000000000.0),
                  detail.getName(),
                  detail.getCondition()));
        }
      }

      if (recordsReport.getTop10MostPeakMemory() != null) {
        builder.append("\n");
        builder.append("operators by peak memory allocated\n");
        builder.append("---------------------------\n");
        final List<OperatorRecordDetail> top10MemoryUsed = recordsReport.getTop10MostPeakMemory();
        int i = 0;
        for (final OperatorRecordDetail detail : top10MemoryUsed) {
          i++;
          builder.append(
              String.format(
                  operatorTemplate,
                  String.format("%1$3s", i),
                  detail.getRecords(),
                  detail.getBatches(),
                  Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                  Human.getHumanDurationFromNanos(detail.getRunTimeNanos()),
                  (detail.getRecords() * 100.0) / (detail.getRunTimeNanos() / 1000000000.0),
                  detail.getName(),
                  detail.getCondition()));
        }
      }
    }
    final Collection<Collection<HtmlTableDataColumn<String, Long>>> memoryByPhaseReport =
        MemoryUsed.generateMemoryByPhaseReport(this.parsed);

    if (!memoryByPhaseReport.isEmpty()) {
      builder.append("\n");
      builder.append("memory usage by phase\n");
      builder.append("---------------------\n");
      for (final Collection<HtmlTableDataColumn<String, Long>> detail : memoryByPhaseReport) {
        final HtmlTableDataColumn<String, Long> phase = Iterables.get(detail, 0);
        final HtmlTableDataColumn<String, Long> usage = Iterables.get(detail, 1);
        builder.append(String.format("phase: %s - usage: %s\n", phase.data(), usage.data()));
      }
    }
    final Collection<Collection<HtmlTableDataColumn<String, Long>>> memoryByPhaseByNodeReport =
        MemoryUsedPerNode.generateMemoryByPhaseReport(this.parsed);
    if (!memoryByPhaseByNodeReport.isEmpty()) {
      builder.append("\n");
      builder.append("memory usage by phase by node\n");
      builder.append("-----------------------------\n");
      for (final Collection<HtmlTableDataColumn<String, Long>> detail : memoryByPhaseByNodeReport) {
        builder.append(
            String.format(
                "phase: %s - node: %s - usage: %s\n",
                Iterables.get(detail, 0).data(),
                Iterables.get(detail, 1).data(),
                Iterables.get(detail, 3).data()));
      }
    }
    builder.append("\n");
    builder.append("row estimate comparisons\n");
    builder.append("------------------------\n");
    final Collection<RowEstimateDetail> estimates =
        RowEstimateReport.getEstimates(this.parsed, planRelations);
    if (estimates.isEmpty()) {
      builder.append("* no row estimates found\n");
    } else {
      estimates.stream()
          .forEach(
              x ->
                  builder.append(
                      String.format(
                          "* %s - %s actual(%,d) estimated(%,.0f) diff(%,.0f)\n",
                          x.getPhaseName(),
                          x.getOpName(),
                          x.getActualRows(),
                          x.getEstimatedRows(),
                          x.getDifferenceRatio())));
    }

    if (showPlanDetails) {
      builder.append("\n");
      builder.append("plan phase timings\n");
      builder.append("------------------\n");

      if (!planRelations.isEmpty()) {
        builder.append("\n");
        builder.append("Plan row count estimates\n");
        builder.append("------------------------\n");
        for (final PlanRelation planRelation : planRelations) {
          addRow(
              builder, planRelation.getOp(), String.format("%,.2f\n", planRelation.getRowCount()));
        }
      }
    }

    builder.append("\n");
    builder.append("block report\n");
    builder.append("------------\n");
    final List<String> blockingBlockingOperatorFindings = new ArrayList<>();
    final MostBlockedReport blockingOperatorReport =
        BlockReport.getBlockingOperatorReport(planRelations, this.parsed);
    if (blockingOperatorReport.getBlockedDownstreamMillis() > 0) {
      final String blockedUpstreamTime =
          Human.getHumanDurationFromMillis(blockingOperatorReport.getBlockedDownstreamMillis());
      final String blockedDownstreamMessage =
          String.format(
              "Phase Thread %s blocked on downstream for %s",
              blockingOperatorReport.getName(), blockedUpstreamTime);
      builder.append("* ");
      builder.append(blockedDownstreamMessage);
      builder.append("\n");

      for (final PhaseBlockStats phaseBlockStats : blockingOperatorReport.getDownstream()) {
        builder.append(
            String.format(
                " | %s downstream max blocked/run/sleep - %s(%.2f%%)/%s(%.2f%%)/%s(%.2f%%)",
                phaseBlockStats.getPhase(),
                Human.getHumanDurationFromMillis(phaseBlockStats.getMaxBlockTime()),
                phaseBlockStats.getMaxBlockTimePercentage(),
                Human.getHumanDurationFromMillis(phaseBlockStats.getRunTime()),
                phaseBlockStats.getRunTimePercentage(),
                Human.getHumanDurationFromMillis(phaseBlockStats.getSleepTime()),
                phaseBlockStats.getSleepTimePercentage()));
        builder.append("\n");
      }
    }
    if (blockingOperatorReport.getBlockedUpstreamMillis() > 0) {
      builder.append("* ");
      final String blockedUpstreamTime =
          Human.getHumanDurationFromMillis(blockingOperatorReport.getBlockedUpstreamMillis());
      final String blockUpstreamMessage =
          String.format(
              "Phase Thread %s is blocked on upstream for %s",
              blockingOperatorReport.getName(), blockedUpstreamTime);
      builder.append(blockUpstreamMessage);
      builder.append("\n");

      for (final PhaseBlockStats phaseBlockStats : blockingOperatorReport.getUpstream()) {
        builder.append(
            String.format(
                " | %s upstream max blocked/run/sleep - %s(%.2f%%)/%s(%.2f%%)/%s(%.2f%%)",
                phaseBlockStats.getPhase(),
                Human.getHumanDurationFromMillis(phaseBlockStats.getMaxBlockTime()),
                phaseBlockStats.getMaxBlockTimePercentage(),
                Human.getHumanDurationFromMillis(phaseBlockStats.getRunTime()),
                phaseBlockStats.getRunTimePercentage(),
                Human.getHumanDurationFromMillis(phaseBlockStats.getSleepTime()),
                phaseBlockStats.getSleepTimePercentage()));
        builder.append("\n");
      }
    }
    if (blockingOperatorReport.getBlockedOnSharedMillis() > 0) {
      final String blockedTime =
          Human.getHumanDurationFromMillis(blockingOperatorReport.getBlockedOnSharedMillis());
      final String blockedOnSharedMessage =
          String.format(
              "Phase %s blocked for %s on shared resources",
              blockingOperatorReport.getName(), blockedTime);
      builder.append("* ");
      builder.append(blockedOnSharedMessage);
      builder.append("\n");
    }
    for (final String finding : blockingBlockingOperatorFindings) {
      builder.append("* ");
      builder.append(finding);
      builder.append("\n");
    }
    builder.append("\n");
    builder.append("findings\n");
    builder.append("--------\n");
    final Collection<String> findings = FindingsReport.searchForFindings(parsed, planRelations);
    if (findings.isEmpty()) {
      builder.append("no findings\n");
    } else {
      for (final String finding : findings) {
        builder.append("* ");
        builder.append(finding);
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  private void addRow(final StringBuilder builder, final String key, final Object value) {
    builder.append(key);
    builder.append(": ");
    builder.append(value);
    builder.append("\n");
  }

  @Override
  public String getTitle() {
    return "Profile Summary Report";
  }
}
