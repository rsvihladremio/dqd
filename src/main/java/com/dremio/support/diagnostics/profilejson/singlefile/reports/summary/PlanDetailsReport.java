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

import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.PlanPhases;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class PlanDetailsReport extends ProfileJSONReport {

  @Override
  protected String createReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    final Collection<PlanDetail> planDetails =
        PlanDetailsReport.getPlanDetails(profileJson, relations);
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    final Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    for (final PlanDetail detail : planDetails) {
      rows.add(
          Arrays.asList(
              HtmlTableDataColumn.col(detail.getName()),
              col(
                  Human.getHumanDurationFromMillis(detail.getDurationMillis()),
                  detail.getDurationMillis()),
              col(
                  String.format("%.2f", detail.getPercentageOfPlanTime()),
                  detail.getPercentageOfPlanTime()),
              col(
                  String.format("%.2f", detail.getPercerntageOfQueryTime()),
                  detail.getPercerntageOfQueryTime())));
    }
    return builder.generateTable(
        "planDetailsTable",
        "Plan Details",
        Arrays.asList("name", "duration", "% of plan", "% of query"),
        rows);
  }

  public static class PlanDetail {
    private String name;
    private long durationMillis;
    private double percentageOfPlanTime;
    private double percerntageOfQueryTime;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getDurationMillis() {
      return durationMillis;
    }

    public void setDurationMillis(long durationMillis) {
      this.durationMillis = durationMillis;
    }

    public double getPercentageOfPlanTime() {
      return percentageOfPlanTime;
    }

    public void setPercentageOfPlanTime(double percentageOfPlanTime) {
      this.percentageOfPlanTime = percentageOfPlanTime;
    }

    public double getPercerntageOfQueryTime() {
      return percerntageOfQueryTime;
    }

    public void setPercerntageOfQueryTime(double percerntageOfQueryTime) {
      this.percerntageOfQueryTime = percerntageOfQueryTime;
    }
  }

  public static Collection<PlanDetail> getPlanDetails(
      ProfileJSON profileJson, Collection<PlanRelation> relations) {
    if (profileJson == null) {
      return new ArrayList<>();
    }
    final List<PlanPhases> planPhases = profileJson.getPlanPhases();
    if (planPhases == null) {
      return new ArrayList<>();
    }
    final long durationMillis = profileJson.getEnd() - profileJson.getStart();
    final long planTime = profileJson.getPlanningEnd() - profileJson.getPlanningStart();
    final List<PlanDetail> planDetails = new ArrayList<>();
    for (final PlanPhases planPhase : planPhases) {
      final PlanDetail planDetail = new PlanDetail();
      planDetail.setName(planPhase.getPhaseName());
      planDetail.setDurationMillis(planPhase.getDurationMillis());
      planDetail.setPercentageOfPlanTime((100.0 * planPhase.getDurationMillis()) / planTime);
      planDetail.setPercerntageOfQueryTime(
          (100.0 * planPhase.getDurationMillis()) / durationMillis);
      planDetails.add(planDetail);
    }
    return planDetails;
  }

  @Override
  public String htmlSectionName() {
    return "plan-details-section";
  }

  @Override
  public String htmlTitle() {
    return "Plan";
  }
}
