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

public class MemoryUsedPerNode extends ProfileJSONReport {

  public static Collection<Collection<HtmlTableDataColumn<String, Long>>>
      generateMemoryByPhaseReport(final ProfileJSON profileJson) {
    if (profileJson == null || profileJson.getFragmentProfile() == null) {
      return new ArrayList<>();
    }
    final Map<String, Map<String, Long>> memoryUsedByPhasePerNode = new HashMap<>();
    for (final FragmentProfile fragmentProfile : profileJson.getFragmentProfile()) {
      if (fragmentProfile.getMinorFragmentProfile() == null) {
        continue;
      }
      for (final MinorFragmentProfile minorFragmentProfile :
          fragmentProfile.getMinorFragmentProfile()) {
        if (minorFragmentProfile.getOperatorProfile() == null
            || minorFragmentProfile.getEndpoint() == null) {
          continue;
        }

        final String node = minorFragmentProfile.getEndpoint().getAddress();
        final Map<String, Long> memoryUsedByPhase;
        if (memoryUsedByPhasePerNode.containsKey(node)) {
          memoryUsedByPhase = memoryUsedByPhasePerNode.get(node);
        } else {
          memoryUsedByPhase = new HashMap<>();
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
          memoryUsedByPhasePerNode.put(node, memoryUsedByPhase);
        }
      }
    }
    final List<Collection<HtmlTableDataColumn<String, Long>>> rows = new ArrayList<>();
    for (final Map.Entry<String, Map<String, Long>> entry : memoryUsedByPhasePerNode.entrySet()) {
      for (final Map.Entry<String, Long> subEntry : entry.getValue().entrySet()) {
        rows.add(
            Arrays.asList(
                HtmlTableDataColumn.col(subEntry.getKey()),
                HtmlTableDataColumn.col(entry.getKey()),
                col(String.valueOf(subEntry.getValue()), subEntry.getValue()),
                col(Human.getHumanBytes1024(subEntry.getValue()), subEntry.getValue())));
      }
    }
    rows.sort(
        (left, right) -> {
          final HtmlTableDataColumn<String, Long> leftMemoryUsed = Iterables.get(left, 2);
          final HtmlTableDataColumn<String, Long> rightMemoryUsed = Iterables.get(right, 2);
          return rightMemoryUsed.sortableData().compareTo(leftMemoryUsed.sortableData());
        });
    return rows;
  }

  @Override
  protected String createReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    Collection<Collection<HtmlTableDataColumn<String, Long>>> rows =
        generateMemoryByPhaseReport(profileJson);
    if (rows.isEmpty()) {
      return "<h2>Total Peak Memory Allocated by Phase By Node</h2><p>no records found</p>";
    }
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    return builder.generateTable(
        "totalPeakMemoryAllocatedByPhaseByNode",
        "Total Peak Memory Allocated by Phase By Node",
        Arrays.asList("phase", "host", "memory bytes", "memory human"),
        rows);
  }

  @Override
  public String htmlSectionName() {
    return "memory-used-per-node-section";
  }

  @Override
  public String htmlTitle() {
    return "Memory Per Node";
  }
}
