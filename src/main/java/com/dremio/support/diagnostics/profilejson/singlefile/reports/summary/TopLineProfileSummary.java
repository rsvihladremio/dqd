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
import static java.util.Arrays.*;

import com.dremio.support.diagnostics.profilejson.QueryState;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.Foreman;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TopLineProfileSummary implements ProfileJSONReport {

  @Override
  public String generateReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    HtmlTableBuilder builder = new HtmlTableBuilder();
    final List<Collection<HtmlTableDataColumn<Object, Object>>> rows = new ArrayList<>();
    String version = profileJson.getDremioVersion();
    rows.add(asList(col("dremio version"), col(version)));
    DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.of("UTC"));
    Instant startTime = Instant.ofEpochMilli(profileJson.getStart());
    rows.add(asList(col("start time"), col(formatter.format(startTime))));
    Instant endTime = Instant.ofEpochMilli(profileJson.getEnd());
    rows.add(asList(col("end time"), col(formatter.format(endTime))));
    rows.add(
        asList(
            col("duration"),
            col(Human.getHumanDurationFromMillis(profileJson.getEnd() - profileJson.getStart()))));
    rows.add(asList(col("user"), col(profileJson.getUser())));
    rows.add(asList(col("query state"), col(QueryState.values()[profileJson.getState()])));
    final String queue;
    if (profileJson.getResourceSchedulingProfile() != null) {
      queue = profileJson.getResourceSchedulingProfile().getQueueName();
    } else {
      queue = "N/A";
    }
    rows.add(asList(col("queue"), col(queue)));
    rows.add(
        asList(
            col("command pool wait time"),
            col(Human.getHumanDurationFromMillis(profileJson.getCommandPoolWaitMillis()))));
    final long phaseCount =
        relations.stream()
            .map(x -> Iterables.get(Splitter.on('-').split(x.getName()), 0))
            .distinct()
            .count();
    rows.add(asList(col("total phases"), col(phaseCount)));
    final long executorNodes;
    if (profileJson.getNodeProfile() != null) {
      executorNodes = profileJson.getNodeProfile().size();
    } else {
      executorNodes = 0;
    }
    rows.add(asList(col("number of executors"), col(executorNodes)));
    final Foreman foreman = profileJson.getForeman();
    if (foreman != null) {
      rows.add(asList(col("coordinator address"), col(foreman.getAddress())));
      rows.add(asList(col("coordinator cores"), col(foreman.getAvailableCores())));
      rows.add(
          asList(
              col("coordinator max direct memory"),
              col(Human.getHumanBytes1024((long) foreman.getMaxDirectMemory()))));
    }
    final String table =
        builder.generateTable(
            "profileSummaryTable", "Profile Summary", asList("name", "value"), rows);
    return "<div><h1>Query</h1><p>" + profileJson.getQuery() + "</p></div>" + table;
  }
}
