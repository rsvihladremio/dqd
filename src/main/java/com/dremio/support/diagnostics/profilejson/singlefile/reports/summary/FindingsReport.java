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
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.RowEstimateReport.RowEstimateDetail;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.StateTimingsReport.StateTiming;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class FindingsReport extends ProfileJSONReport {

  private final double percentageQueryThreshold = 5.0;

  @Override
  protected String createReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    HtmlTableBuilder builder = new HtmlTableBuilder();
    List<Collection<HtmlTableDataColumn<String, Number>>> findings = new ArrayList<>();
    for (String finding : FindingsReport.searchForFindings(profileJson, relations)) {
      findings.add(Collections.singletonList(new HtmlTableDataColumn<>(finding, null, false)));
    }
    return builder.generateTable(
        "findingsTable", "Findings", Collections.singletonList(""), findings);
  }

  public static Collection<String> searchForFindings(
      ProfileJSON profileJson, Collection<PlanRelation> relations) {
    final List<String> findings = new ArrayList<>();
    final FindingsReport report = new FindingsReport();
    Collection<RowEstimateDetail> estimates =
        RowEstimateReport.getEstimates(profileJson, relations);
    findings.addAll(report.getPhasesWithIncorrectEstimate(profileJson, estimates));
    // findings.addAll(report.getSignificantSingleThreadedOperations(profileJson,
    // relations));
    // findings.addAll(report.getAllJoinsThatAreReversedIncorrectly(profileJson,
    // relations));
    findings.addAll(report.getTimeConsumedFinding(profileJson));
    findings.addAll(report.getClientBlocking(profileJson));
    findings.addAll(report.getAllNestedLoopJoins(relations));
    findings.addAll(report.getPartitionPruning(profileJson, estimates, relations));
    return findings;
  }

  private Collection<String> getClientBlocking(final ProfileJSON profileJson) {
    if (profileJson.getFragmentProfile() == null) {
      return new ArrayList<>();
    }
    final List<String> result = new ArrayList<>();
    for (final FragmentProfile fragmentProfile : profileJson.getFragmentProfile()) {
      if (fragmentProfile == null || fragmentProfile.getMinorFragmentProfile() == null) {
        continue;
      }
      for (final MinorFragmentProfile minorFragmentProfile :
          fragmentProfile.getMinorFragmentProfile()) {
        if (minorFragmentProfile == null || minorFragmentProfile.getOperatorProfile() == null) {
          continue;
        }
        for (final OperatorProfile operatorProfile : minorFragmentProfile.getOperatorProfile()) {
          if (fragmentProfile.getMajorFragmentId() == 0 && operatorProfile.getOperatorId() == 0) {
            if (minorFragmentProfile.getBlockedOnDownstreamDuration() > 0) {
              long duration = profileJson.getEnd() - profileJson.getStart();
              final double percentOfQuery;
              if (duration == 0 || minorFragmentProfile.getBlockedOnDownstreamDuration() == 0) {
                percentOfQuery = 0.0;
              } else {
                percentOfQuery =
                    minorFragmentProfile.getBlockedOnDownstreamDuration() * 100.0 / duration;
              }
              if (percentOfQuery > percentageQueryThreshold) {
                result.add(
                    String.format(
                        "Phase 00-%s-00 is blocked by the client for %s which is %.2f%% of query"
                            + " time",
                        StringUtils.leftPad(
                            String.valueOf(minorFragmentProfile.getMinorFragmentId()), 2, "0"),
                        Human.getHumanDurationFromMillis(
                            minorFragmentProfile.getBlockedOnDownstreamDuration()),
                        percentOfQuery));
              }
            }
          }
        }
      }
    }
    return result;
  }

  private Collection<String> getTimeConsumedFinding(final ProfileJSON profileJSON) {
    final Collection<StateTiming> stateTimings = StateTimingsReport.getStateTimings(profileJSON);
    List<StateTiming> sorted =
        stateTimings.stream()
            .sorted(
                (left, right) -> Long.compare(right.getDurationMillis(), left.getDurationMillis()))
            .toList();
    final List<StateTiming> responsibleTimings = new ArrayList<>();
    final double threshold = 80.0;
    double queryPercentageFound = 0.0;
    for (final StateTiming stateTiming : sorted) {
      if (queryPercentageFound > threshold) {
        break;
      }
      responsibleTimings.add(stateTiming);
      queryPercentageFound += stateTiming.getPercentageOfQueryTime();
    }
    final String responsibleStateTimingsString =
        responsibleTimings.stream()
            .map(x -> String.format("(%s %.2f%%)", x.getName(), x.getPercentageOfQueryTime()))
            .collect(Collectors.joining(", "));
    return Collections.singletonList(
        String.format(
            "%.2f %% of the query time is taken up by the following phases [%s]",
            queryPercentageFound, responsibleStateTimingsString));
  }

  private Collection<String> getPhasesWithIncorrectEstimate(
      final ProfileJSON profileJson, final Collection<RowEstimateDetail> rowEstimates) {
    final Set<String> matching = new HashSet<>();
    final double threshold = 10.0;
    final long queryDuration = profileJson.getEnd() - profileJson.getStart();
    for (final RowEstimateDetail detail : rowEstimates) {
      double operatorRuntimeMS = detail.getMaxOperatorRuntimeNanos() / 1000000.0;
      double perc = operatorRuntimeMS * 100.0 / queryDuration;
      if (detail.getDifferenceRatio() > threshold && perc > percentageQueryThreshold) {
        matching.add("%s-XX-%s".formatted(detail.getPhaseName(), detail.getOpName()));
      }
    }
    if (matching.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(
        "Op(s) with row estimate inaccuracy > 10%% and query time > 5%%): %s"
            .formatted(String.join(", ", matching)));
  }

  private Collection<String> getAllNestedLoopJoins(final Collection<PlanRelation> relations) {
    Set<String> nlpPhases = new HashSet<>();
    for (PlanRelation relation : relations) {
      if (relation.getOp().contains("NestedLoopJoin")) {

        nlpPhases.add(relation.getName());
      }
    }
    if (nlpPhases.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(
        "Nested Loop joins in phases: %s".formatted(String.join(",", nlpPhases)));
  }

  private Collection<String> getPartitionPruning(
      final ProfileJSON profileJson,
      final Collection<RowEstimateDetail> estimates,
      Collection<PlanRelation> relations) {
    List<String> findings = new ArrayList<>();
    Map<String, Set<String>> tablesWithPruning = new HashMap<>();
    Map<String, Set<String>> tablesWithoutPruningAndSignificantQueryTime = new HashMap<>();
    for (PlanRelation relation : relations) {
      if (relation.getOp().contains("IcebergManifestListPrel")) {
        if (relation.getValues().keySet().stream()
            .anyMatch(x -> x.contains("ManifestList Filter Expression"))) {
          String tableName = String.valueOf(relation.getValues().get("table"));
          var relationName = relation.getName();
          if (tablesWithPruning.containsKey(tableName)) {
            final Set<String> tableRelations = tablesWithPruning.get(tableName);
            tableRelations.add(relationName);
            tablesWithPruning.put(tableName, tableRelations);
          } else {
            final Set<String> tableRelations = new HashSet<>();
            tableRelations.add(relationName);
            tablesWithPruning.put(tableName, tableRelations);
          }
        } else {
          final Optional<RowEstimateDetail> maybeEstimate =
              estimates.stream()
                  .filter(x -> x.getPhaseName().equals(relation.getName()))
                  .findFirst();
          final RowEstimateDetail rowEstimateDetail;
          if (maybeEstimate.isPresent()) {
            rowEstimateDetail = maybeEstimate.get();
            final double perc;
            long queryDuration = profileJson.getEnd() - profileJson.getStart();
            if (rowEstimateDetail.getMaxOperatorRuntimeNanos() > 0 && queryDuration > 0) {
              perc =
                  (rowEstimateDetail.getMaxOperatorRuntimeNanos() / 1000000.0 / queryDuration)
                      * 100.0;
            } else {
              perc = 0.0;
            }
            if (perc > percentageQueryThreshold)
              if (rowEstimateDetail.getActualRows() > 0) {
                String tableName = String.valueOf(relation.getValues().get("table"));
                var relationName = relation.getName();
                if (tablesWithoutPruningAndSignificantQueryTime.containsKey(tableName)) {
                  final Set<String> tableRelations = tablesWithPruning.get(tableName);
                  tableRelations.add(relationName);
                  tablesWithoutPruningAndSignificantQueryTime.put(tableName, tableRelations);
                } else {
                  final Set<String> tableRelations = new HashSet<>();
                  tableRelations.add(relationName);
                  tablesWithoutPruningAndSignificantQueryTime.put(tableName, tableRelations);
                }
              }
          }
        }
      }
    }
    if (!tablesWithPruning.isEmpty()) {
      var finding = new StringBuilder();
      finding.append("The following tables have pruning: ");
      var lines = new ArrayList<String>();
      for (var kvp : tablesWithPruning.entrySet()) {
        var table = kvp.getKey();
        lines.add("%s phases (%s)".formatted(table, String.join(", ", kvp.getValue())));
      }
      finding.append(String.join(";", lines));
      findings.add(finding.toString());
    }

    if (!tablesWithoutPruningAndSignificantQueryTime.isEmpty()) {
      var finding = new StringBuilder();
      finding.append("The following tables have no pruning: ");
      var lines = new ArrayList<String>();
      for (var kvp : tablesWithoutPruningAndSignificantQueryTime.entrySet()) {
        var table = kvp.getKey();
        lines.add("%s phases (%s)".formatted(table, String.join(", ", kvp.getValue())));
      }
      finding.append(String.join(";", lines));
      findings.add(finding.toString());
    }

    return findings;
  }

  @Override
  public String htmlSectionName() {
    return "findings-report-section";
  }

  @Override
  public String htmlTitle() {
    return "Findings";
  }
}
