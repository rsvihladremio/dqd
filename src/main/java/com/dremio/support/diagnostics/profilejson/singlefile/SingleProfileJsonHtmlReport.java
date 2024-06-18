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
package com.dremio.support.diagnostics.profilejson.singlefile;

import com.dremio.support.diagnostics.profilejson.*;
import com.dremio.support.diagnostics.profilejson.PhaseThread;
import com.dremio.support.diagnostics.profilejson.converttorel.ConvertToRelGraph;
import com.dremio.support.diagnostics.profilejson.converttorel.ConvertToRelGraphParser;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelationshipParser;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileSummaryReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.plots.OperatorDurationPlot;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.plots.OperatorRecordsPlot;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.plots.PhasesPlot;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.plots.TimelinePlot;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import com.dremio.support.diagnostics.shared.Report;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SingleProfileJsonHtmlReport implements Report {

  private final ProfileJSON parsed;
  private final boolean showConvertToRel;
  private final boolean showPlanDetails;
  private final GraphWriter sankeyWriter = new GraphWriter();
  private static final JsLibraryTextProvider jsLibProvider = new JsLibraryTextProvider();

  /**
   * Generates some graphs to display visual information not included in the summary
   *
   * @param showPlanDetails display all the plan details that are visible
   * @param showConvertToRel when true will display the convert to rel graph assuming there are not
   *     too many phases (default 100) that need to be displayed
   * @param parsed the ProfileJSON object full parsed
   */
  public SingleProfileJsonHtmlReport(
      final boolean showPlanDetails, final boolean showConvertToRel, final ProfileJSON parsed) {
    this.showPlanDetails = showPlanDetails;
    this.showConvertToRel = showConvertToRel;
    this.parsed = parsed;
  }

  /**
   * generates custom html based on the data inside the ProfileJson that was passed to the ctor
   *
   * @return html as a string ready to hand off the web server
   */
  @Override
  public String getText() {
    final List<PhaseThread> phaseThreads = getPhaseThreads();
    final long[] startTimes = new long[phaseThreads.size()];
    final long[] endTimes = new long[phaseThreads.size()];
    final String[] phaseThreadNames = new String[phaseThreads.size()];
    final String[] phaseThreadTextNames = new String[phaseThreads.size()];
    final long[] phaseProcessTimes = new long[phaseThreads.size()];
    for (int i = 0; i < phaseThreads.size(); i++) {
      final PhaseThread phaseThread = phaseThreads.get(i);
      startTimes[i] = phaseThread.getStartTime();
      endTimes[i] = phaseThread.getEndTime();
      phaseThreadNames[i] = String.format("%02d", phaseThread.getPhaseId());
      phaseThreadTextNames[i] =
          String.format(
              "%02d-%02d-XX - run %s, sleep %s, blocked { total %s, upstream %s, downstream %s,"
                  + " shared %s }",
              phaseThread.getPhaseId(),
              phaseThread.getThreadId(),
              Human.getHumanDurationFromMillis(phaseThread.getRunDuration()),
              Human.getHumanDurationFromMillis(phaseThread.getSleepingDuration()),
              Human.getHumanDurationFromMillis(phaseThread.getBlockedDuration()),
              Human.getHumanDurationFromMillis(phaseThread.getBlockedOnUpstreamDuration()),
              Human.getHumanDurationFromMillis(phaseThread.getBlockedOnDownstreamDuration()),
              Human.getHumanDurationFromMillis(phaseThread.getBlockedOnSharedResourceDuration()));
      phaseProcessTimes[i] = phaseThread.getTotalTimeMillis();
    }
    final List<String> scripts = new ArrayList<>();
    final List<String> htmlFragments = new ArrayList<>();
    final List<Operator> operators = new ArrayList<>();
    if (this.parsed != null) {
      final Collection<PlanRelation> planRelations =
          new PlanRelationshipParser().getPlanRelations(this.parsed);
      htmlFragments.add(
          new ProfileSummaryReport()
              .generateSummary(this.showPlanDetails, this.parsed, planRelations));
      // disable dynamic graphs if page is too large
      final int maxThreadsForGraphs = 500;
      if (phaseThreadNames.length < maxThreadsForGraphs) {
        final String plotlyJsText = jsLibProvider.getPlotlyJsText();
        scripts.add("<script>" + plotlyJsText + "</script>");
        final String mermaidJsText = jsLibProvider.getMermaidJsText();
        scripts.add("<script>" + mermaidJsText + "</script>");
        htmlFragments.add(
            new PhasesPlot()
                .generatePlot(phaseThreadNames, phaseProcessTimes, phaseThreadTextNames));
        htmlFragments.add(
            new TimelinePlot()
                .generatePlot(phaseThreadNames, startTimes, endTimes, phaseThreadTextNames));
        // graph out operators by process time
        if (this.parsed.getFragmentProfile() != null) {
          for (final FragmentProfile fragmentProfile : this.parsed.getFragmentProfile()) {
            if (fragmentProfile != null && fragmentProfile.getMinorFragmentProfile() != null) {
              final int phaseId = fragmentProfile.getMajorFragmentId();
              for (final MinorFragmentProfile minorProfile :
                  fragmentProfile.getMinorFragmentProfile()) {
                if (minorProfile != null && minorProfile.getOperatorProfile() != null) {
                  for (final OperatorProfile operatorProfile : minorProfile.getOperatorProfile()) {
                    final Operator operator =
                        Operator.createFromOperatorProfile(
                            operatorProfile,
                            this.parsed.getOperatorTypeMetricsMap().getMetricsDef());
                    operator.setParentPhaseId(phaseId);
                    operator.setThreadId(minorProfile.getMinorFragmentId());
                    operators.add(operator);
                  }
                }
              }
            }
          }
        }
      }
      final String[] operatorNames = new String[operators.size()];
      final String[] operatorText = new String[operators.size()];
      final long[] operatorTimes = new long[operators.size()];
      final long[] operatorRecords = new long[operators.size()];
      for (int i = 0; i < operators.size(); i++) {
        final Operator operator = operators.get(i);
        // calculate relative id number to provide a clean layout with only phases
        // labeled using the
        // prefix feature
        operatorNames[i] = String.format("%02d", operator.getParentPhaseId());
        operatorText[i] =
            String.format(
                "%s { records: %s batches: %s setup: %s wait: %s process: %s }",
                String.format(
                    "%s %02d-%02d-%02d",
                    operator.getKind(),
                    operator.getParentPhaseId(),
                    operator.getThreadId(),
                    operator.getId()),
                operator.getRecords(),
                operator.getBatches(),
                Human.getHumanDurationFromMillis(operator.getSetupMillis()),
                Human.getHumanDurationFromMillis(operator.getWaitMillis()),
                Human.getHumanDurationFromMillis(operator.getProcessTimeMillis()));
        operatorTimes[i] = operator.getTotalTimeMillis();
        operatorRecords[i] = operator.getRecords();
      }

      htmlFragments.add(
          new OperatorDurationPlot().generatePlot(operatorNames, operatorTimes, operatorText));
      htmlFragments.add(
          new OperatorRecordsPlot().generatePlot(operatorNames, operatorRecords, operatorText));
      final String convertToRel;
      if (showConvertToRel) {
        final ConvertToRelGraph c = new ConvertToRelGraphParser().parseConvertToRel(parsed);
        if (c != null) {
          convertToRel =
              "<h2>Convert To Rel</h2>\n" + sankeyWriter.writeMermaid(c.getConvertToRelTree());
        } else {
          convertToRel = "";
        }
      } else {
        convertToRel = "";
      }
      htmlFragments.add(convertToRel);
    } else {
      htmlFragments.add(
          "<h3 style=\"color: red\">Too Many Phases: Disabled Graphs and Convert To Rel</h3>");
    }

    return "<!doctype html>\n"
        + "<html   lang=\"en\">\n"
        + "<head>\n"
        + "  <meta charset=\"utf-8\">\n"
        + "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        + "  <title>"
        + this.getTitle()
        + " </title>\n"
        + "  <meta name\"description\" content=\"report for "
        + this.getTitle()
        + " \">\n"
        + "  <meta name=\"author\" content=\"dremio\">\n"
        + "  <meta property=\"og:title\" content=\""
        + this.getTitle()
        + "\">\n"
        + "  <meta property=\"og:type\" content=\"website\">\n"
        + "  <meta property=\"og:description\" content=\"plotly generated graphs\">\n"
        + "<style>\n"
        + "caption {\n"
        + "font-weight: bold;\n"
        + "font-size: 24px;\n"
        + "text-align: left;\n"
        + "color: #333;\n"
        + "	margin-bottom: 16px;\n"
        + "	margin-top: 16px;\n"
        + "}\n"
        + "table {\n"
        + "border-collapse: collapse;\n"
        + "text-align: center;\n"
        + "vertical-align: middle;\n"
        + "}\n"
        + "th, td {\n"
        + "border: 1px solid black;\n"
        + "padding: 8px;\n"
        + "}\n"
        + "thead {\n"
        + "background-color: #333;\n"
        + "color: white;\n"
        + "font-size: 0.875rem;\n"
        + "text-transform: uppercase;\n"
        + "letter-spacing: 2%;\n"
        + "}\n"
        + "tbody tr:nth-child(odd) {\n"
        + "background-color: #fff;\n"
        + "}\n"
        + "tbody tr:nth-child(even) {\n"
        + "background-color: #eee;\n"
        + "}\n"
        + "tbody th {\n"
        + "background-color: #36c;\n"
        + "color: #fff;\n"
        + " text-align: left;\n"
        + "}\n"
        + "tbody tr:nth-child(even) th {\n"
        + "background-color: #25c;\n"
        + "}\n"
        + "</style>\n"
        + " <style> \n"
        + "  .mermaidTooltip { \n"
        + "    position: absolute; \n"
        + "    text-align: center; \n"
        + "    max-width: 200px; \n"
        + "    padding: 2px; \n"
        + "    font-family: 'trebuchet ms', verdana, arial; \n"
        + "    font-size: 12px; \n"
        + "    background: #ffffde;\n"
        + "    border: 1px solid #aaaa33;\n"
        + "    border-radius: 2px;\n"
        + "    pointer-events: none;\n"
        + "    z-index: 100; \n"
        + "}\n"
        + " </style>\n"
        + "<style>\n"
        + jsLibProvider.getSortableCSSText()
        + "</style>\n"
        + "<script>\n"
        + jsLibProvider.getSortableText()
        + "</script>\n"
        + "<script>\n"
        + jsLibProvider.getCSVExportText()
        + "</script>\n"
        + "<script>\n"
        + jsLibProvider.getFilterTableText()
        + "</script>\n"
        + String.join("\n", scripts)
        + "</head>\n"
        + "<body>"
        + String.join("\n", htmlFragments)
        + "</body>";
  }

  private List<PhaseThread> getPhaseThreads() {
    final List<PhaseThread> phaseThreads = new ArrayList<>();
    if (this.parsed != null && this.parsed.getFragmentProfile() != null) {
      for (final FragmentProfile phase : this.parsed.getFragmentProfile()) {
        if (phase != null && phase.getMinorFragmentProfile() != null) {
          final int phaseId = phase.getMajorFragmentId();
          for (final MinorFragmentProfile phaseThread : phase.getMinorFragmentProfile()) {
            final long threadId = phaseThread.getMinorFragmentId();
            final PhaseThread pt = getPhaseThread(phaseThread, phaseId, threadId);
            phaseThreads.add(pt);
          }
        }
      }
    }
    return phaseThreads;
  }

  private static PhaseThread getPhaseThread(
      MinorFragmentProfile phaseThread, int phaseId, long threadId) {
    final PhaseThread pt = new PhaseThread();
    pt.setPhaseId(phaseId);
    pt.setThreadId(threadId);
    pt.setRunDuration(phaseThread.getRunDuration());
    pt.setBlockedDuration(phaseThread.getBlockedDuration());
    pt.setBlockedOnUpstreamDuration(phaseThread.getBlockedOnUpstreamDuration());
    pt.setBlockedOnDownstreamDuration(phaseThread.getBlockedOnDownstreamDuration());
    pt.setBlockedOnSharedResourceDuration(phaseThread.getBlockedOnSharedResourceDuration());
    pt.setSleepingDuration(phaseThread.getSleepingDuration());
    pt.setTotalTimeMillis(phaseThread.getEndTime() - phaseThread.getStartTime());
    pt.setEndTime(phaseThread.getEndTime());
    pt.setStartTime(phaseThread.getEndTime() - pt.getRunDuration());
    return pt;
  }

  public ProfileJSON getParsed() {
    return parsed;
  }

  @Override
  public String getTitle() {
    return "Profile.json Analysis";
  }
}
