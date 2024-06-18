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
package com.dremio.support.diagnostics.profilejson.singlefile.reports.summary;

import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.BlockFinder;
import com.dremio.support.diagnostics.profilejson.singlefile.PhaseBlockStats;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class BlockReport implements ProfileJSONReport {

  private final BlockFinder blockFinder = new BlockFinder();
  private final HtmlTableBuilder tableBuilder = new HtmlTableBuilder();

  @Override
  public String generateReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    final MostBlockedReport blockingOperatorReport =
        BlockReport.getBlockingOperatorReport(relations, profileJson);
    if (blockingOperatorReport == null) {
      return "";
    }
    final StringBuilder builder = new StringBuilder();
    final String mostBlockedPhaseName = blockingOperatorReport.getName();

    List<Collection<HtmlTableDataColumn<String, Long>>> mostBlockedData = new ArrayList<>();
    mostBlockedData.add(
        Arrays.asList(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockingOperatorReport.getBlockedUpstreamMillis()),
                blockingOperatorReport.getBlockedDownstreamMillis()),
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(
                    blockingOperatorReport.getBlockedDownstreamMillis()),
                blockingOperatorReport.getBlockedDownstreamMillis()),
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockingOperatorReport.getBlockedOnSharedMillis()),
                blockingOperatorReport.getBlockedOnSharedMillis())));
    final String headerTable =
        tableBuilder.generateTable(
            "mostBlockedPhase",
            String.format("Most blocked phase %s", mostBlockedPhaseName),
            Arrays.asList(
                "upstream blocked time", "downstream blocked time", "shared blocked time"),
            mostBlockedData);
    builder.append(headerTable);
    if (blockingOperatorReport.getBlockedUpstreamMillis() > 0) {
      final Collection<Collection<HtmlTableDataColumn<String, Number>>> upstreamRows =
          new ArrayList<>();
      for (PhaseBlockStats blockStats : blockingOperatorReport.getUpstream()) {
        List<HtmlTableDataColumn<String, Number>> cols = new ArrayList<>();
        cols.add(new HtmlTableDataColumn<>(blockStats.getPhase(), null));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockStats.getMaxBlockTime()),
                blockStats.getMaxBlockTime()));
        cols.add(
            new HtmlTableDataColumn<>(
                String.format("%.2f%%", blockStats.getMaxBlockTimePercentage()),
                blockStats.getMaxBlockTimePercentage()));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockStats.getSleepTime()),
                blockStats.getSleepTime()));
        cols.add(
            new HtmlTableDataColumn<>(
                String.format("%.2f%%", blockStats.getSleepTimePercentage()),
                blockStats.getSleepTimePercentage()));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockStats.getRunTime()),
                blockStats.getRunTime()));
        cols.add(
            new HtmlTableDataColumn<>(
                String.format("%.2f%%", blockStats.getRunTimePercentage()),
                blockStats.getRunTimePercentage()));
        upstreamRows.add(cols);
      }
      builder.append(
          tableBuilder.generateTable(
              "upstreamBlockage",
              String.format(
                  "Phase %s upstream blockage (trying to pull from source)", mostBlockedPhaseName),
              Arrays.asList(
                  "phase",
                  "max block time",
                  String.format("block %% of phase %s block", mostBlockedPhaseName),
                  "max sleep time",
                  String.format("sleep %% of phase %s sleep", mostBlockedPhaseName),
                  "max run time",
                  String.format("run %% of phase %s run", mostBlockedPhaseName)),
              upstreamRows));
    }
    if (blockingOperatorReport.getBlockedDownstreamMillis() > 0) {
      final Collection<Collection<HtmlTableDataColumn<String, Number>>> downstreamRows =
          new ArrayList<>();
      for (PhaseBlockStats blockStats : blockingOperatorReport.getDownstream()) {
        List<HtmlTableDataColumn<String, Number>> cols = new ArrayList<>();
        cols.add(new HtmlTableDataColumn<>(blockStats.getPhase(), null));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockStats.getMaxBlockTime()),
                blockStats.getMaxBlockTime()));
        cols.add(
            new HtmlTableDataColumn<>(
                String.format("%.2f%%", blockStats.getMaxBlockTimePercentage()),
                blockStats.getMaxBlockTimePercentage()));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockStats.getSleepTime()),
                blockStats.getSleepTime()));
        cols.add(
            new HtmlTableDataColumn<>(
                String.format("%.2f%%", blockStats.getSleepTimePercentage()),
                blockStats.getSleepTimePercentage()));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(blockStats.getRunTime()),
                blockStats.getRunTime()));
        cols.add(
            new HtmlTableDataColumn<>(
                String.format("%.2f%%", blockStats.getRunTimePercentage()),
                blockStats.getRunTimePercentage()));
        downstreamRows.add(cols);
      }
      if (downstreamRows.isEmpty() && mostBlockedPhaseName.equals("00-00-xx")) {
        List<HtmlTableDataColumn<String, Number>> cols = new ArrayList<>();
        cols.add(new HtmlTableDataColumn<>("JDBC/ODBC/REST/ETC CLIENT", null));
        cols.add(new HtmlTableDataColumn<>("0 millis", 0));
        cols.add(new HtmlTableDataColumn<>("0.0%", 0.0));
        cols.add(new HtmlTableDataColumn<>("0 millis", 0));
        cols.add(new HtmlTableDataColumn<>("0.0%", 0.0));
        cols.add(
            new HtmlTableDataColumn<>(
                Human.getHumanDurationFromMillis(
                    blockingOperatorReport.getBlockedDownstreamMillis()),
                blockingOperatorReport.getBlockedDownstreamMillis()));
        cols.add(new HtmlTableDataColumn<>(String.format("%.2f%%", 100.0), 100.0));
        downstreamRows.add(cols);
      }
      builder.append(
          tableBuilder.generateTable(
              "phaseDownstreamBlockage",
              String.format(
                  "Phase %s downstream blockage (trying to push towards the client)",
                  mostBlockedPhaseName),
              Arrays.asList(
                  "phase",
                  "max block time",
                  String.format("block %% of phase %s block", mostBlockedPhaseName),
                  "max sleep time",
                  String.format("sleep %% of phase %s sleep", mostBlockedPhaseName),
                  "max run time",
                  String.format("run %% of phase %s run", mostBlockedPhaseName)),
              downstreamRows));
    }
    return builder.toString();
  }

  public static class MostBlockedReport {
    private Collection<PhaseBlockStats> upstream;
    private Collection<PhaseBlockStats> downstream;
    private long blockedOnSharedMillis;
    private long blockedUpstreamMillis;
    private long blockedDownstreamMillis;
    private String name;

    public Collection<PhaseBlockStats> getUpstream() {
      return upstream;
    }

    public void setUpstream(Collection<PhaseBlockStats> upstream) {
      this.upstream = upstream;
    }

    public Collection<PhaseBlockStats> getDownstream() {
      return downstream;
    }

    public void setDownstream(Collection<PhaseBlockStats> downstream) {
      this.downstream = downstream;
    }

    public long getBlockedOnSharedMillis() {
      return blockedOnSharedMillis;
    }

    public void setBlockedOnSharedMillis(long blockedOnSharedMillis) {
      this.blockedOnSharedMillis = blockedOnSharedMillis;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getBlockedUpstreamMillis() {
      return blockedUpstreamMillis;
    }

    public void setBlockedUpstreamMillis(long blockedUpstreamMillis) {
      this.blockedUpstreamMillis = blockedUpstreamMillis;
    }

    public long getBlockedDownstreamMillis() {
      return blockedDownstreamMillis;
    }

    public void setBlockedDownstreamMillis(long blockedDownstreamMillis) {
      this.blockedDownstreamMillis = blockedDownstreamMillis;
    }
  }

  private Collection<String> getAllUpstreamPhasesForName(
      final Collection<PlanRelation> planRelations, final String name) {
    List<String> matches = new ArrayList<>();
    for (PlanRelation planRelation : planRelations) {
      if (planRelation.getName().equals(name)) {
        for (PlanRelation upstream : planRelation.getUpstream()) {
          final String upStreamName = upstream.getName();
          final String phaseOnly = Iterables.get(Splitter.on('-').split(upStreamName), 0);
          matches.add(phaseOnly);
          for (final String e : getAllUpstreamPhasesForName(planRelations, upStreamName)) {
            matches.add(Iterables.get(Splitter.on('-').split(e), 0));
          }
        }
      }
    }
    return matches;
  }

  private Collection<String> getAllDownstreamPhasesForName(
      final Collection<PlanRelation> planRelations, final String name) {
    Set<String> matches = new LinkedHashSet<>();
    for (PlanRelation planRelation : planRelations) {
      if (planRelation.getName().equals(name)) {
        for (PlanRelation downStream : planRelation.getDownstream()) {
          final String downStreamName = downStream.getName();
          final String phaseOnly = Iterables.get(Splitter.on('-').split(downStreamName), 0);
          matches.add(phaseOnly);
          for (final String e : getAllDownstreamPhasesForName(planRelations, downStreamName)) {
            matches.add(Iterables.get(Splitter.on('-').split(e), 0));
          }
        }
      }
    }
    return matches;
  }

  public static MostBlockedReport getBlockingOperatorReport(
      Collection<PlanRelation> planRelations, ProfileJSON profileJSON) {
    return new BlockReport().getBlockingOperator(planRelations, profileJSON);
  }

  public MostBlockedReport getBlockingOperator(
      Collection<PlanRelation> planRelations, ProfileJSON profileJSON) {
    MinorFragmentProfile mostBlocked = null;
    long mostBlockedDuration = 0;
    List<String> blockedPhaseOperatorNames = new ArrayList<>();
    String fullName = "";
    String blockedPhaseName = "";
    if (profileJSON.getFragmentProfile() == null) {
      return new MostBlockedReport();
    }
    for (FragmentProfile fragment : profileJSON.getFragmentProfile()) {
      if (fragment == null || fragment.getMinorFragmentProfile() == null) {
        continue;
      }
      for (MinorFragmentProfile minorFragment : fragment.getMinorFragmentProfile()) {
        if (minorFragment == null) {
          continue;
        }
        long blockedDuration = minorFragment.getBlockedDuration();
        if (blockedDuration > mostBlockedDuration) {
          mostBlocked = minorFragment;
          mostBlockedDuration = blockedDuration;
          blockedPhaseOperatorNames = new ArrayList<>();
          final String phaseName =
              StringUtils.leftPad(String.valueOf(fragment.getMajorFragmentId()), 2, "0");
          String threadId =
              StringUtils.leftPad(String.valueOf(minorFragment.getMinorFragmentId()), 2, "0");
          fullName = String.format("%s-%s-xx", phaseName, threadId);
          blockedPhaseName = phaseName;
          if (minorFragment.getOperatorProfile() == null) {
            continue;
          }
          for (OperatorProfile operatorProfile : minorFragment.getOperatorProfile()) {
            String operatorId =
                StringUtils.leftPad(String.valueOf(operatorProfile.getOperatorId()), 2, "0");
            blockedPhaseOperatorNames.add(String.format("%s-%s", phaseName, operatorId));
          }
        }
      }
    }
    if (mostBlocked == null) {
      return new MostBlockedReport();
    }
    final MostBlockedReport blockedReport = new MostBlockedReport();
    blockedReport.setName(fullName);
    blockedReport.setBlockedDownstreamMillis(mostBlocked.getBlockedOnDownstreamDuration());
    blockedReport.setBlockedUpstreamMillis(mostBlocked.getBlockedOnUpstreamDuration());
    blockedReport.setBlockedOnSharedMillis(mostBlocked.getBlockedOnSharedResourceDuration());
    if (mostBlocked.getBlockedOnDownstreamDuration() > 0) {
      final Set<String> downstreamPhases = new LinkedHashSet<>();
      for (final String blockedPhaseOperatorName : blockedPhaseOperatorNames) {
        final Collection<String> downstream =
            getAllDownstreamPhasesForName(planRelations, blockedPhaseOperatorName);
        downstreamPhases.addAll(downstream);
      }
      final List<PhaseBlockStats> downstream = new ArrayList<>();
      for (String phase : downstreamPhases) {
        // skip the actual phase
        if (phase.equals(blockedPhaseName)) {
          continue;
        }
        downstream.add(blockFinder.getDownstreamPhaseBlockStats(phase, mostBlocked, profileJSON));
      }
      downstream.sort(Comparator.comparing(PhaseBlockStats::getRunTime).reversed());
      blockedReport.setDownstream(downstream);
    }
    if (mostBlocked.getBlockedOnUpstreamDuration() > 0) {
      final Set<String> upstreamPhases = new LinkedHashSet<>();
      for (final String blockedPhaseOperatorName : blockedPhaseOperatorNames) {
        final Collection<String> upstream =
            getAllUpstreamPhasesForName(planRelations, blockedPhaseOperatorName);
        upstreamPhases.addAll(upstream);
      }

      final List<PhaseBlockStats> upstream = new ArrayList<>();
      for (String phase : upstreamPhases) {
        // skip the actual phase
        if (phase.equals(blockedPhaseName)) {
          continue;
        }
        upstream.add(blockFinder.getUpstreamPhaseBlockStats(phase, mostBlocked, profileJSON));
      }
      upstream.sort(Comparator.comparing(PhaseBlockStats::getRunTime).reversed());
      blockedReport.setUpstream(upstream);
    }

    if (mostBlocked.getBlockedOnSharedResourceDuration() > 0) {
      blockedReport.setBlockedOnSharedMillis(mostBlocked.getBlockedOnSharedResourceDuration());
    }
    return blockedReport;
  }
}
