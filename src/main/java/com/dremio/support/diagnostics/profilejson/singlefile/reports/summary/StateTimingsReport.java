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

import com.dremio.support.diagnostics.profilejson.JobState;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StateTimingsReport implements ProfileJSONReport {

  @Override
  public String generateReport(
      final ProfileJSON profileJson, final Collection<PlanRelation> relations) {
    final Collection<StateTiming> stateTimings = StateTimingsReport.getStateTimings(profileJson);
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    for (final StateTiming entry : stateTimings) {
      rows.add(
          Arrays.asList(
              HtmlTableDataColumn.col(entry.getName()),
              col(
                  Human.getHumanDurationFromMillis(entry.getDurationMillis()),
                  entry.getDurationMillis()),
              col(
                  String.format("%.2f%%", entry.getPercentageOfQueryTime()),
                  entry.getPercentageOfQueryTime())));
    }
    return builder.generateTable(
        "stateTimingsTable",
        "State Timings",
        Arrays.asList("state", "duration", "% of query time"),
        rows);
  }

  public static class StateTiming {
    private String name;
    private long durationMillis;
    private double percentageOfQueryTime;

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

    public double getPercentageOfQueryTime() {
      return percentageOfQueryTime;
    }

    public void setPercentageOfQueryTime(double percentageOfQueryTime) {
      this.percentageOfQueryTime = percentageOfQueryTime;
    }
  }

  public static Collection<StateTiming> getStateTimings(final ProfileJSON profileJson) {
    final List<StateTiming> stateTimings = new ArrayList<>();
    long jobDuration = profileJson.getEnd() - profileJson.getStart();
    long lastStartTime = profileJson.getStart();
    final Map<ProfileState, Long> profileStateMap = new LinkedHashMap<>();
    if (profileJson.getStateList() == null) {
      return new ArrayList<>();
    }
    for (final ProfileState state : profileJson.getStateList()) {
      long duration = state.getStartTime() - lastStartTime;
      lastStartTime = state.getStartTime();
      profileStateMap.put(state, duration);
    }
    for (final JobState jobState :
        Arrays.asList(
            JobState.PENDING,
            JobState.METADATA_RETRIEVAL,
            JobState.PLANNING,
            JobState.ENGINE_START,
            JobState.QUEUED,
            JobState.EXECUTION_PLANNING,
            JobState.STARTING,
            JobState.RUNNING)) {
      final int jobStateid = jobState.ordinal();
      long duration = 0;
      // "N/A";
      for (final Map.Entry<ProfileState, Long> e : profileStateMap.entrySet()) {
        final int stateId = e.getKey().getState();
        if (stateId == jobStateid) {
          duration = e.getValue();
        }
      }
      StateTiming timing = new StateTiming();
      timing.setName(jobState.name());
      timing.setDurationMillis(duration);
      if (duration > 0 && jobDuration > 0) {
        timing.setPercentageOfQueryTime((100.0 * duration) / jobDuration);
      }
      stateTimings.add(timing);
    }
    return stateTimings;
  }
}
