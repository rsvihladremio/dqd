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

import com.dremio.support.diagnostics.profilejson.CoreOperatorType;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class MemoryUsed extends ProfileJSONReport {

  public static Collection<Collection<HtmlTableDataColumn<String, Long>>>
      generateMemoryByPhaseReport(final ProfileJSON profileJson) {
    if (profileJson == null || profileJson.getFragmentProfile() == null) {
      return new ArrayList<>();
    }
    final Map<String, Long> memoryUsedByPhase = new HashMap<>();
    for (final FragmentProfile fragmentProfile : profileJson.getFragmentProfile()) {
      if (fragmentProfile.getMinorFragmentProfile() == null) {
        continue;
      }
      for (final MinorFragmentProfile minorFragmentProfile :
          fragmentProfile.getMinorFragmentProfile()) {
        if (minorFragmentProfile.getOperatorProfile() == null) {
          continue;
        }
        for (final OperatorProfile operatorProfile : minorFragmentProfile.getOperatorProfile()) {
          final String phaseName =
              String.format(
                  "%s-%s %s",
                  StringUtils.leftPad(String.valueOf(fragmentProfile.getMajorFragmentId()), 2, "0"),
                  StringUtils.leftPad(String.valueOf(operatorProfile.getOperatorId()), 2, "0"),
                  CoreOperatorType.values()[operatorProfile.getOperatorType()]);
          if (memoryUsedByPhase.containsKey(phaseName)) {
            final long total = memoryUsedByPhase.get(phaseName);
            memoryUsedByPhase.put(phaseName, total + operatorProfile.getPeakLocalMemoryAllocated());
          } else {
            memoryUsedByPhase.put(phaseName, operatorProfile.getPeakLocalMemoryAllocated());
          }
        }
      }
    }
    final List<Collection<HtmlTableDataColumn<String, Long>>> rows = new ArrayList<>();
    for (final Map.Entry<String, Long> entry : memoryUsedByPhase.entrySet()) {
      final String key = entry.getKey();
      final Long value = entry.getValue();

      final HtmlTableDataColumn<String, Long> first = new HtmlTableDataColumn<>(key, null, false);
      final HtmlTableDataColumn<String, Long> second =
          new HtmlTableDataColumn<>(Human.getHumanBytes1024(value), value, false);
      final List<HtmlTableDataColumn<String, Long>> row = Arrays.asList(first, second);
      rows.add(row);
    }
    rows.sort(
        (left, right) -> {
          HtmlTableDataColumn<String, Long> leftColumn = Iterables.get(left, 1);
          HtmlTableDataColumn<String, Long> rightColumn = Iterables.get(right, 1);
          return rightColumn.sortableData().compareTo(leftColumn.sortableData());
        });
    return rows;
  }

  @Override
  protected String createReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    Collection<Collection<HtmlTableDataColumn<String, Long>>> rows =
        generateMemoryByPhaseReport(profileJson);
    if (rows.isEmpty()) {
      return "<h2>Total Peak Memory Allocated by Phase (across nodes)</h2><p>no records found</p>";
    }
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    return builder.generateTable(
        "totalPeakMemoryAllocatedByPhase",
        "Total Peak Memory Allocated by Phase (across nodes)",
        Arrays.asList("Phase", "memory"),
        rows);
  }

  @Override
  public String htmlSectionName() {
    return "memory-used-section";
  }

  @Override
  public String htmlTitle() {
    return "Memory";
  }
}
