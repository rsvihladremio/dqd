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
package com.dremio.support.diagnostics.profilejson;

import com.dremio.support.diagnostics.profilejson.converttorel.ConvertToRelGraph;
import com.dremio.support.diagnostics.profilejson.converttorel.ConvertToRelGraphParser;
import com.dremio.support.diagnostics.profilejson.singlefile.GraphWriter;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import com.dremio.support.diagnostics.shared.Report;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class HtmlProfileComparisonReport implements Report {

  private final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();
  private final ConvertToRelGraphParser convertToRelGraphParser = new ConvertToRelGraphParser();
  private final boolean showConvertToRel;

  private final ProfileJSON parsed;
  private final ProfileJSON parsed2;

  private final ProfileJsonComparisonTextReport consoleReport;

  public HtmlProfileComparisonReport(
      final boolean showConvertToRel,
      final ProfileJSON parsed1,
      final ProfileJSON parsed2,
      final ProfileJsonComparisonTextReport consoleReport) {
    this.showConvertToRel = showConvertToRel;
    if (parsed1 == null) {
      throw new InvalidParameterException("profile1 cannot be null, this is a critical bug");
    }
    this.parsed = parsed1;
    if (parsed2 == null) {
      throw new InvalidParameterException("profile2 cannot be null, this is a critical bug");
    }
    this.parsed2 = parsed2;
    if (consoleReport == null) {
      throw new InvalidParameterException(
          "the console report cannot be null, this is a critical bug");
    }
    this.consoleReport = consoleReport;
  }

  @Override
  public String getText() throws IOException {
    // print out same summary that is available in console report
    final String summary =
        "<div style=\"white-space:pre-wrap;font-family:monospace\">"
            + this.consoleReport.getText()
            + "</div>";
    final TraceData profile2TraceData = convertToPhaseThreads(this.parsed2);
    final String profile2PhaseProcessTrace =
        writeTrace(
            "profile2PhaseProcessTrace",
            "profile 2 phase thread duration millis",
            profile2TraceData.getPhaseThreadNames(),
            profile2TraceData.getPhaseProcessTimes(),
            Optional.of(profile2TraceData.getPhaseThreadTextNames()));
    final TraceData profile1TraceData = convertToPhaseThreads(this.parsed);
    final String phaseProcessTrace =
        writeTrace(
            "profile1PhaseProcessTrace",
            "profile 1 phase thread duration millis",
            profile1TraceData.getPhaseThreadNames(),
            profile1TraceData.getPhaseProcessTimes(),
            Optional.of(profile1TraceData.getPhaseThreadTextNames()));
    final Optional<String[]> barMode = Optional.empty();
    final String phasesPlot =
        writePlot(
            "Thread Duration by Phase",
            "Duration milliseconds",
            "phasesThreadsDuration",
            new String[] {"profile1PhaseProcessTrace", "profile2PhaseProcessTrace"},
            new String[] {phaseProcessTrace, profile2PhaseProcessTrace},
            100,
            barMode);
    // graph out operators by process time
    final TraceData profile2OperatorTraceData = getTraceDataForOperators(this.parsed2);
    final String profile2OperatorTrace =
        writeTrace(
            "profile2OperatorTrace",
            "profile 2 operator duration",
            profile2OperatorTraceData.getPhaseThreadNames(),
            profile2OperatorTraceData.getPhaseProcessTimes(),
            Optional.of(profile2OperatorTraceData.getPhaseThreadTextNames()));
    final TraceData profile1OperatorTraceData = getTraceDataForOperators(this.parsed);
    final String operatorTrace =
        writeTrace(
            "profile1OperatorTrace",
            "profile 1 operator duration",
            profile1OperatorTraceData.getPhaseThreadNames(),
            profile1OperatorTraceData.getPhaseProcessTimes(),
            Optional.of(profile1OperatorTraceData.getPhaseThreadTextNames()));
    final String operatorPlot =
        writePlot(
            "Operators Duration by Phase",
            "Duration Milliseconds (Setup+Process+Wait Time)",
            "operatorsDuration",
            new String[] {
              "profile1OperatorTrace", "profile2OperatorTrace",
            },
            new String[] {operatorTrace, profile2OperatorTrace},
            50,
            Optional.empty());
    final long profile1EndTime = Arrays.stream(profile1TraceData.getEndTimes()).max().orElse(0L);
    final long profile1StartTime =
        Arrays.stream(profile1TraceData.getStartTimes()).min().orElse(0L);
    final long profile1Duration = profile1EndTime - profile1StartTime;
    final long profile2EndTime = Arrays.stream(profile2TraceData.getEndTimes()).max().orElse(0L);
    final long profile2StartTime =
        Arrays.stream(profile2TraceData.getStartTimes()).min().orElse(0L);
    final long profile2Duration = profile2EndTime - profile2StartTime;
    final long end = Math.max(profile1Duration, profile2Duration);
    final String profile1Timeline =
        candleStick(
            "profile1Timeline",
            "Profile 1 Thread Timeline",
            end,
            profile1TraceData.getPhaseThreadNames(),
            profile1TraceData.getStartTimes(),
            profile1TraceData.getEndTimes(),
            profile1TraceData.getPhaseThreadTextNames(),
            "rgb(31,119,180)");
    final String profile2Timeline =
        candleStick(
            "profile2Timeline",
            "Profile 2 Thread Timeline",
            end,
            profile2TraceData.getPhaseThreadNames(),
            profile2TraceData.getStartTimes(),
            profile2TraceData.getEndTimes(),
            profile2TraceData.getPhaseThreadTextNames(),
            "orange");
    final String convertToRel1;
    final String convertToRel2;
    if (showConvertToRel) {
      final ConvertToRelGraph convertToRelProfile1 =
          convertToRelGraphParser.parseConvertToRel(this.parsed);
      if (convertToRelProfile1 != null) {
        convertToRel1 =
            "<h2>Convert To Rel Profile 1</h2>"
                + new GraphWriter().writeMermaid(convertToRelProfile1.getConvertToRelTree());
      } else {
        convertToRel1 = "<h2>Convert To Rel Profile 2</h2><p>No Convert To Rel Found</p>";
      }

      final ConvertToRelGraph convertToRelProfile2 =
          convertToRelGraphParser.parseConvertToRel(this.parsed2);
      if (convertToRelProfile2 != null) {
        convertToRel2 =
            "<h2>Convert To Rel Profile 2</h2>"
                + new GraphWriter().writeMermaid(convertToRelProfile2.getConvertToRelTree());
      } else {
        convertToRel2 = "<h2>Convert To Rel Profile 2</h2><p>No Convert To Rel Found</p>";
      }
    } else {
      convertToRel1 = "";
      convertToRel2 = "";
    }
    return "<!doctype html>\n"
        + "<html   lang=\"en\">\n"
        + "<head>\n"
        + "  <meta charset=\"utf-8\">\n"
        + "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        + "  <title>Profile Comparison Report</title>\n"
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
        + "  <meta name\"description\" content=\"report for "
        + this.getTitle()
        + " \">\n"
        + "  <meta name=\"author\" content=\"dremio\">\n"
        + "  <meta property=\"og:title\" content=\""
        + this.getTitle()
        + "\">\n"
        + "  <meta property=\"og:type\" content=\"website\">\n"
        + "  <meta property=\"og:description\" content=\"plotly generated graphs\">\n"
        + "<script>"
        + jsLibraryTextProvider.getPlotlyJsText()
        + "</script>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getMermaidJsText()
        + "</script>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getCSVExportText()
        + "</script>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getFilterTableText()
        + "</script>\n"
        + "<script>\n"
        + jsLibraryTextProvider.getFilterTableText()
        + "</script>\n"
        + "<style>\n"
        + jsLibraryTextProvider.getSortableCSSText()
        + "</style>\n"
        + "</head>\n"
        + "<body>"
        + summary
        + phasesPlot
        + operatorPlot
        + profile1Timeline
        + profile2Timeline
        + "<div style=\"display: grid;\" grid-template-columns: repeat(2, 1fr); gap: 10px;"
        + " grid-auto-rows: minmax(100px, auto);\">\n"
        + "<div style=\"grid-column: 1/2; grid-row: 1;\">\n"
        + convertToRel1
        + "</div>\n"
        + "<div style=\"grid-column: 2/2; grid-row: 1;\">\n"
        + convertToRel2
        + "</div>\n"
        + "</div>\n"
        + "</body>";
  }

  @Override
  public String getTitle() {
    return "Profile.json Analysis";
  }

  private String candleStick(
      final String id,
      final String title,
      final long end,
      final String[] phases,
      final long[] startTimes,
      final long[] endTimes,
      final String[] textLabels,
      final String color) {
    final boolean allLengthsMatch =
        (startTimes.length == endTimes.length) && (endTimes.length == phases.length);
    if (!allLengthsMatch) {
      throw new RuntimeException(
          "critical bug in generating start/stop chart we cannot have a different number of start"
              + " and stop times");
    }
    final StringBuilder data = new StringBuilder();
    if (startTimes.length > 0) {
      final long firstStartTime = Arrays.stream(startTimes).min().getAsLong();
      for (int i = 0; i < startTimes.length; i++) {
        final long startTime = startTimes[i];
        final long endTime = endTimes[i];
        final String phase = phases[i];
        final String label = textLabels[i];
        data.append("data.push({type: 'scatter',x: [\"")
            .append(startTime - firstStartTime)
            .append("\",\"")
            .append(endTime - firstStartTime)
            .append("\"],")
            .append("y: [\"")
            .append(phase)
            .append("\",\"")
            .append(phase)
            .append("\"],")
            .append("text: [\"")
            .append("RUN START ")
            .append(label)
            .append("\",\"")
            .append("RUN END ")
            .append(label)
            .append("\"],")
            .append("mode: 'lines+markers',marker: {color: '")
            .append(color)
            .append("'}")
            .append("}")
            .append(");\n");
      }
    }
    return "<div id=\""
        + id
        + "\"/>\n<script>"
        + "var data = [];"
        + data
        + "var layout = {"
        + "xaxis: { title: 'Milliseconds since query start', range: [0,"
        + end
        + "]},"
        + "yaxis: { title: 'Phases', type: 'category'},"
        + "title: '"
        + title
        + "',"
        + "showlegend: false"
        + "};\n"
        + "Plotly.newPlot(\""
        + id
        + "\", data, layout);\n</script>\n";
  }

  private String writeTrace(
      final String traceId,
      final String title,
      final String[] x,
      final long[] y,
      final Optional<String[]> textLabels) {
    final StringBuilder builder = new StringBuilder();
    builder.append("var ");
    builder.append(traceId);
    builder.append("= { y: [");
    for (final String e : x) {
      builder.append("\"");
      builder.append(e);
      builder.append("\",");
    }
    builder.append("],");
    builder.append("\nx: [");
    for (final long e : y) {
      builder.append(e);
      builder.append(",");
    }
    builder.append("], ");
    if (textLabels.isPresent()) {
      builder.append("text: [");
      for (final String e : textLabels.get()) {
        builder.append("\"");
        builder.append(e);
        builder.append("\",");
      }
      builder.append("], ");
    }
    builder.append(" orientation: 'h', mode: '" + "markers" + "', xaxis: 'x', yaxis: 'y', type: '");
    builder.append("scatter");
    builder.append("', name:'");
    builder.append(title);
    builder.append("',};");
    return builder.toString();
  }

  private String writePlot(
      final String title,
      final String xAxisTitle,
      final String elementId,
      final String[] traceIds,
      final String[] traces,
      final int labelMargin,
      final Optional<String[]> layoutFields) {
    final StringBuilder builder = new StringBuilder();
    builder.append("<div id='");
    builder.append(elementId);
    builder.append("' />");
    builder.append("<script type=\"text/javascript\">\n");
    builder.append("var ");
    builder.append(elementId);
    builder.append(" = document.getElementById('");
    builder.append(elementId);
    builder.append("');\n");
    for (final String trace : traces) {
      builder.append(trace);
    }
    builder.append("\nvar data = [");
    builder.append(String.join(", ", traceIds));
    builder.append("];");
    builder
        .append("\n" + "var layout = { " + "  margin: { l: ")
        .append(labelMargin)
        .append(" }, yaxis: {title: '");
    builder.append("Phases");
    builder.append("', type: 'category'}, xaxis: {title: '");
    builder.append(xAxisTitle);
    builder.append("'");
    builder.append(", tickangle: -60 }, title: '");
    builder.append(title);
    builder.append("'");
    if (layoutFields.isPresent()) {
      for (final String f : layoutFields.get()) {
        builder.append(", ");
        builder.append(f);
      }
    }
    builder.append("};");
    builder.append("\nPlotly.newPlot(");
    builder.append(elementId);
    builder.append(",data , layout");
    builder.append(");\n</script>");
    return builder.toString();
  }

  private TraceData convertToPhaseThreads(final ProfileJSON parsed) {
    final List<PhaseThread> phaseThreads = new ArrayList<>();
    if (parsed.getFragmentProfile() == null) {
      return new TraceData(
          new String[] {}, new String[] {}, new long[] {}, new long[] {}, new long[] {});
    }
    for (final FragmentProfile phase : parsed.getFragmentProfile()) {
      final int phaseId = phase.getMajorFragmentId();
      for (final MinorFragmentProfile phaseThread : phase.getMinorFragmentProfile()) {
        final long threadId = phaseThread.getMinorFragmentId();
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
        pt.setStartTime(phaseThread.getEndTime() - phaseThread.getRunDuration());
        pt.setEndTime(phaseThread.getEndTime());
        phaseThreads.add(pt);
      }
    }
    final long[] startTimes = new long[phaseThreads.size()];
    final long[] endTimes = new long[phaseThreads.size()];
    final String[] phaseThreadNames = new String[phaseThreads.size()];
    final String[] phaseThreadTextNames = new String[phaseThreads.size()];
    final long[] phaseProcessTimes = new long[phaseThreads.size()];
    for (int i = 0; i < phaseThreads.size(); i++) {
      final PhaseThread phaseThread = phaseThreads.get(i);
      phaseThreadNames[i] = String.format("%02d", phaseThread.getPhaseId());
      phaseThreadTextNames[i] =
          String.format(
              "%02d-%02d-XX - run %s, sleep %s, blocked { total %s, upstream %s, downwstream %s,"
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
      startTimes[i] = phaseThread.getStartTime();
      endTimes[i] = phaseThread.getEndTime();
    }
    return new TraceData(
        phaseThreadNames, phaseThreadTextNames, phaseProcessTimes, startTimes, endTimes);
  }

  private TraceData getTraceDataForOperators(final ProfileJSON parsed) {
    final List<Operator> operators = new ArrayList<>();
    if (parsed.getFragmentProfile() == null) {
      return new TraceData(
          new String[] {}, new String[] {}, new long[] {}, new long[] {}, new long[] {});
    }
    for (final FragmentProfile fragmentProfile : parsed.getFragmentProfile()) {
      final int phaseId = fragmentProfile.getMajorFragmentId();
      for (final MinorFragmentProfile minorProfile : fragmentProfile.getMinorFragmentProfile()) {
        final long threadId = minorProfile.getMinorFragmentId();
        for (final OperatorProfile operatorProfile : minorProfile.getOperatorProfile()) {
          final Operator operator =
              Operator.createFromOperatorProfile(
                  operatorProfile, parsed.getOperatorTypeMetricsMap().getMetricsDef());
          operator.setParentPhaseId(phaseId);
          operator.setThreadId(threadId);
          operators.add(operator);
        }
      }
    }
    final String[] operatorNames = new String[operators.size()];
    final String[] operatorText = new String[operators.size()];
    final long[] operatorTimes = new long[operators.size()];
    final TraceData traceData =
        new TraceData(operatorNames, operatorText, operatorTimes, new long[] {}, new long[] {});
    for (int i = 0; i < operators.size(); i++) {
      final Operator operator = operators.get(i);
      // calculate relative id number to provide a clean layout with only phases labeled using the
      // prefix feature
      operatorNames[i] = String.format("%02d", operator.getParentPhaseId());
      operatorText[i] =
          String.format(
              "%s %02d-%02d-%02d { records: %s, batches: %s, setup: %s, wait: %s, process: %s}",
              operator.getKind(),
              operator.getParentPhaseId(),
              operator.getThreadId(),
              operator.getId(),
              operator.getRecords(),
              operator.getBatches(),
              Human.getHumanDurationFromMillis(operator.getSetupMillis()),
              Human.getHumanDurationFromMillis(operator.getWaitMillis()),
              Human.getHumanDurationFromMillis(operator.getProcessTimeMillis()));
      operatorTimes[i] = operator.getTotalTimeMillis();
    }
    return traceData;
  }
}
