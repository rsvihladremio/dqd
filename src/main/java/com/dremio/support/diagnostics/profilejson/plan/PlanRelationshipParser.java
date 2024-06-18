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
package com.dremio.support.diagnostics.profilejson.plan;

import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlanRelationshipParser {
  private final Pattern costPattern =
      Pattern.compile(
          "\\{(?<rows>(.*)) rows, (?<cpu>(.*)) cpu, (?<io>(.*)) io, (?<network>(.*)) network,"
              + " (?<memory>(.*)) memory}");

  // private final Pattern isNestedRow =
  // Pattern.compile("RecordType\\((?<data>(.*))\\) (?<columnName>(.*))");
  // private final Pattern recordTypePattern =
  // Pattern.compile("(?<type>^\\w*)(?<precision>\\(.*\\))?\\s?(?<complex>\\w*)?
  // (?<name>.*$)");
  private final Map<String, PlanRelation> knownInstances = new HashMap<>();

  /**
   * Summarizes and parses a plan to break to read the important parts in the plan for analysis of
   * the query
   *
   * @param profileJSON the profile json to search for plan relationships
   * @return a list of plans and their relationships
   */
  public List<PlanRelation> getPlanRelations(final ProfileJSON profileJSON) {
    if (profileJSON.getJsonPlan() == null) {
      return new ArrayList<>();
    }
    final ObjectMapper mapper = new ObjectMapper();
    final String jsonString = profileJSON.getJsonPlan().replaceAll("\\n", "\n").replace("\\\"", "");

    Map<String, PlanNode> map;
    try {
      map =
          mapper.readValue(
              jsonString.getBytes(StandardCharsets.UTF_8),
              new TypeReference<Map<String, PlanNode>>() {});
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    List<PlanRelation> planRelations = new ArrayList<>();
    for (final Map.Entry<String, PlanNode> kvp : map.entrySet()) {
      planRelations.add(fromPlanNode(kvp.getKey(), kvp.getValue(), map));
    }
    for (PlanRelation rel : planRelations) {
      mapUpstream(rel, map);
    }
    return planRelations;
  }

  private CumulativeCost getCumulativeCost(final String rawCumulativeCost) {
    if (rawCumulativeCost.equals("{tiny}")) {
      return new CumulativeCost(0d, 0d, 0d, 0d, 0d);
    }
    final Matcher match = costPattern.matcher(rawCumulativeCost);
    if (match.find()) {
      final String rowsRaw = match.group("rows");
      final double rows = Double.parseDouble(rowsRaw);
      final String cpuRaw = match.group("cpu");
      final double cpu = Double.parseDouble(cpuRaw);
      final String ioRaw = match.group("io");
      final double io = Double.parseDouble(ioRaw);
      final String networkRaw = match.group("network");
      final double network = Double.parseDouble(networkRaw);
      final String memoryRaw = match.group("memory");
      final double memory = Double.parseDouble(memoryRaw);
      return new CumulativeCost(rows, cpu, io, network, memory);
    }
    throw new RuntimeException("unable to parse '" + rawCumulativeCost + "'");
  }

  private PlanRelation fromPlanNode(
      final String name, final PlanNode planNode, final Map<String, PlanNode> map) {
    if (knownInstances.containsKey(name)) {
      return knownInstances.get(name);
    }
    PlanRelation rel =
        new PlanRelation(
            name.replaceAll("\"", ""),
            planNode.getOp(),
            planNode.getValues(),
            findDownstream(name, map),
            new ArrayList<>(),
            planNode.getRowCount(),
            getCumulativeCost(planNode.getCumulativeCost()));
    knownInstances.put(name, rel);
    return rel;
  }

  private List<PlanRelation> findDownstream(final String name, final Map<String, PlanNode> map) {
    List<PlanRelation> parents = new ArrayList<>();
    for (final Map.Entry<String, PlanNode> entry : map.entrySet()) {
      if (entry.getValue().inputs.contains(name)) {
        parents.add(fromPlanNode(entry.getKey(), entry.getValue(), map));
      }
    }
    return parents;
  }

  private void mapUpstream(final PlanRelation parent, final Map<String, PlanNode> map) {
    PlanNode planNode = map.get(parent.getName());
    for (final String child : planNode.inputs) {
      for (final Map.Entry<String, PlanNode> entry : map.entrySet()) {
        if (entry.getKey().equals(child)) {
          final PlanNode childNode = entry.getValue();
          final PlanRelation childConvertToRel = fromPlanNode(child, childNode, map);
          parent.getUpstream().add(childConvertToRel);
        }
      }
    }
  }
}
