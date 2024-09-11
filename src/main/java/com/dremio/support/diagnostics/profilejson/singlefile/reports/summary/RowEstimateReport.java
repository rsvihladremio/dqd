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

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;

import com.dremio.support.diagnostics.profilejson.CoreOperatorType;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.InputProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class RowEstimateReport extends ProfileJSONReport {

  private static final Logger logger = Logger.getLogger(RowEstimateReport.class.getName());

  @Override
  protected String createReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    final Collection<RowEstimateDetail> details =
        RowEstimateReport.getEstimates(profileJson, relations);

    final Collection<Collection<HtmlTableDataColumn<String, Number>>> rows =
        details.stream()
            .map(
                x -> {
                  final double perc;
                  if (profileJson != null && (profileJson.getEnd() - profileJson.getStart()) > 0) {
                    final long duration = profileJson.getEnd() - profileJson.getStart();
                    perc = (x.getMaxOperatorRuntimeNanos() / 1000000.0) / duration;
                  } else {
                    perc = 0.0;
                  }
                  Collection<HtmlTableDataColumn<String, Number>> result =
                      Arrays.asList(
                          HtmlTableDataColumn.col(
                              String.format("%s %s", x.getPhaseName(), x.getOpName())),
                          col(String.format("%,.0f", x.getEstimatedRows()), x.getEstimatedRows()),
                          col(String.format("%,d", x.getActualRows()), x.getActualRows()),
                          col(
                              String.format("%,.0fx", x.getDifferenceRatio()),
                              x.getDifferenceRatio()),
                          col(
                              Human.getHumanDurationFromNanos(x.getMaxOperatorRuntimeNanos()),
                              x.getMaxOperatorRuntimeNanos()),
                          col(String.format("%.0f%%", perc * 100.0), perc));
                  return result;
                })
            .toList();
    return new HtmlTableBuilder()
        .generateTable(
            "rowEstimateVariationsTable",
            "Row Estimate Variations",
            Arrays.asList(
                "phase name",
                "estimated",
                "actual",
                "difference ratio",
                "max operator runtime",
                "% of query duration"),
            rows);
  }

  public static class RowEstimateDetail {
    private String phaseName;
    private double estimatedRows;
    private long actualRows;
    private String opName;
    private long maxOperatorRuntime;

    public String getPhaseName() {
      return phaseName;
    }

    public double getDifferenceRatio() {
      final BigDecimal max =
          new BigDecimal(this.actualRows).max(new BigDecimal(this.estimatedRows));
      final BigDecimal min =
          new BigDecimal(this.actualRows).min(new BigDecimal(this.estimatedRows));
      if (max.compareTo(BigDecimal.ZERO) == 0 || min.compareTo(BigDecimal.ZERO) == 0) {
        return Double.NaN;
      }
      final BigDecimal diff = max.divide(min, RoundingMode.HALF_UP);
      return diff.doubleValue();
    }

    public void setPhaseName(String phaseName) {
      this.phaseName = phaseName;
    }

    public double getEstimatedRows() {
      return estimatedRows;
    }

    public void setEstimatedRows(double estimatedRows) {
      this.estimatedRows = estimatedRows;
    }

    public long getActualRows() {
      return actualRows;
    }

    public void setActualRows(long actualRows) {
      this.actualRows = actualRows;
    }

    public void setOpName(String opName) {
      this.opName = opName;
    }

    public String getOpName() {
      return this.opName;
    }

    public void setMaxOperatorRuntimeNanos(long operatorRuntime) {
      this.maxOperatorRuntime = operatorRuntime;
    }

    public long getMaxOperatorRuntimeNanos() {
      return this.maxOperatorRuntime;
    }
  }

  public static Collection<RowEstimateDetail> getEstimates(
      ProfileJSON profileJson, Collection<PlanRelation> relations) {
    if (profileJson == null || profileJson.getFragmentProfile() == null) {
      return Collections.emptyList();
    }
    final Map<String, RowEstimateDetail> phasesByRecords = new HashMap<>();
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
          if (operatorProfile == null || operatorProfile.getInputProfile() == null) {
            continue;
          }
          final String phaseName =
              String.format(
                  "%s-%s",
                  StringUtils.leftPad(String.valueOf(fragmentProfile.getMajorFragmentId()), 2, "0"),
                  StringUtils.leftPad(String.valueOf(operatorProfile.getOperatorId()), 2, "0"));
          final long operatorRuntime = operatorProfile.getProcessNanos();
          final String opName =
              CoreOperatorType.values()[operatorProfile.getOperatorType()].toString();
          long records = 0;
          for (final InputProfile inputProfile : operatorProfile.getInputProfile()) {
            records += inputProfile.getRecords();
          }
          if (phasesByRecords.containsKey(phaseName)) {
            final RowEstimateDetail detail = phasesByRecords.get(phaseName);
            detail.setActualRows(detail.getActualRows() + records);
            if (operatorRuntime > detail.getMaxOperatorRuntimeNanos()) {
              detail.setMaxOperatorRuntimeNanos(operatorRuntime);
            }
            phasesByRecords.put(phaseName, detail);
          } else {
            final RowEstimateDetail detail = new RowEstimateDetail();
            detail.setOpName(opName);
            detail.setPhaseName(phaseName);
            detail.setActualRows(records);
            detail.setMaxOperatorRuntimeNanos(operatorRuntime);
            phasesByRecords.put(phaseName, detail);
          }
        }
      }
    }
    return relations.stream()
        .map(
            relation -> {
              final RowEstimateDetail detail;
              if (phasesByRecords.containsKey(relation.getName())) {
                detail = phasesByRecords.get(relation.getName());
              } else {
                logger.info(
                    () ->
                        String.format(
                            "%s was not found in phases by records list of %s, assuming 0 records",
                            relation.getName(), phasesByRecords.keySet()));
                detail = new RowEstimateDetail();
                detail.setPhaseName(relation.getName());
                detail.setOpName("Not Run");
              }
              final double rowEstimate = relation.getRowCount();
              detail.setEstimatedRows(rowEstimate);
              return detail;
            })
        .sorted(
            (left, right) -> Double.compare(right.getDifferenceRatio(), left.getDifferenceRatio()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public String htmlSectionName() {
    return "row-estimate-section";
  }

  @Override
  public String htmlTitle() {
    return "Row Estimates";
  }
}
