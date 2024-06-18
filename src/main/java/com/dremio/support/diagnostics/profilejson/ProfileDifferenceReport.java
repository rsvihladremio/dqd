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

import static com.dremio.support.diagnostics.shared.Human.getHumanDurationFromMillis;

import com.dremio.support.diagnostics.shared.dto.profilejson.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import com.google.common.base.Splitter;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** generates a difference report between two profiles */
public class ProfileDifferenceReport implements Differ {

  static Map<String, String> splitTopLevelKeys(final String text) {
    // this works with our weird little semi json format
    final Map<String, String> ret = new HashMap<>();
    StringBuilder currentKey = new StringBuilder();
    StringBuilder currentValue = new StringBuilder();
    int openTags = 0;
    int closedTags = 0;
    int openParens = 0;
    int closedParens = 0;
    boolean addToKey = true;
    // skip end and beginning characters..these are { } respectively we don't care
    for (int i = 1; i < text.length() - 1; i++) {
      // access each character
      final char c = text.charAt(i);
      if (c == '(') {
        openParens += 1;
      }
      if (c == ')') {
        closedParens += 1;
      }
      if (c == '{') {
        openTags += 1;
      }
      if (c == '}') {
        closedTags += 1;
      }
      if (c == ',' && openTags == closedTags && openParens == closedParens) {
        // time to close out key
        ret.put(currentKey.toString().trim(), currentValue.toString().trim());
        currentKey = new StringBuilder();
        currentValue = new StringBuilder();
        addToKey = true;
        continue;
      }

      if (c == '=' && openTags == closedTags && openParens == closedParens) {
        addToKey = false;
        continue;
      }
      if (addToKey) {
        currentKey.append(c);
      } else {
        currentValue.append(c);
      }
    }
    // get last values
    if (!currentKey.toString().trim().isEmpty()) {
      ret.put(currentKey.toString().trim(), currentValue.toString().trim());
    }
    return ret;
  }

  private static boolean scanOperators(final OperatorProfile operatorProfile) {
    return operatorTypeFromId(operatorProfile.getOperatorType()).name().endsWith("_SCAN");
  }

  private static boolean noOpOperator(final OperatorProfile operatorProfile) {
    return true;
  }

  private static CoreOperatorType operatorTypeFromId(final int id) {
    return CoreOperatorType.values()[id];
  }

  private static Operator findSlowestOperator(
      final ProfileJSON profile, final Predicate<OperatorProfile> filter) {
    final List<Operator> operators = new ArrayList<>();
    if (profile.getFragmentProfile() != null) {
      for (final FragmentProfile fragment : profile.getFragmentProfile()) {
        if (fragment.getMinorFragmentProfile() != null)
          for (final MinorFragmentProfile minorFragment : fragment.getMinorFragmentProfile()) {
            if (minorFragment.getOperatorProfile() != null) {
              for (final OperatorProfile operator : minorFragment.getOperatorProfile()) {
                if (filter.test(operator) && profile.getOperatorTypeMetricsMap() != null) {
                  final Operator scanOperator =
                      Operator.createFromOperatorProfile(
                          operator, profile.getOperatorTypeMetricsMap().getMetricsDef());
                  operators.add(scanOperator);
                }
              }
            }
          }
        operators.sort(Comparator.comparing(Operator::getTotalTimeMillis).reversed());
      }
    }
    if (operators.isEmpty()) {
      return null;
    }
    return operators.get(0);
  }

  private String escapeForVersion24Plus(String json) {
    if (json == null) {
      return null;
    }
    return json.replace("\r", "")
        .replace("\\r", "")
        .replace("\\\\n", "")
        .replace("\\n", "")
        .replace("\\\\\\\"", "\\\"");
  }

  @Override
  public List<Difference> getDifferences(
      final String profile1Path,
      final String profile2Path,
      final boolean showPlanningDetail,
      final ProfileJSON profile1,
      final ProfileJSON profile2) {
    final List<Difference> differences = new ArrayList<>();

    // compare planning
    final ObjectMapper mapper = new ObjectMapper();
    final TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {};

    Map<String, Object> profile1Plan = new HashMap<>();
    Map<String, Object> profile2Plan = new HashMap<>();
    final String profile1PlanStr = escapeForVersion24Plus(profile1.getJsonPlan());
    if (profile1PlanStr != null) {
      try {
        profile1Plan = mapper.readValue(profile1PlanStr, typeRef);
      } catch (final JsonProcessingException e) {
        throw new InvalidJsonException(e, profile1Path);
      }
    }

    final String profile2PlanStr = escapeForVersion24Plus(profile2.getJsonPlan());
    if (profile2PlanStr != null) {
      try {
        profile2Plan = mapper.readValue(profile2PlanStr, typeRef);
      } catch (final JsonProcessingException e) {
        throw new InvalidJsonException(e, profile1Path);
      }
    }

    final List<String> keys = new ArrayList<>(profile1Plan.keySet());
    // add any keys missing
    for (final String key : profile2Plan.keySet()) {
      if (!keys.contains(key)) {
        keys.add(key);
      }
    }
    final Comparator<String> sComparator =
        (a, b) -> {
          final List<String> aTokens = Splitter.on('-').splitToList(a.replace("\"", ""));
          final List<String> bTokens = Splitter.on('-').splitToList(b.replace("\"", ""));
          final int a1 = Integer.parseInt(aTokens.get(0));
          final int b1 = Integer.parseInt(bTokens.get(0));
          if (a1 > b1) {
            return 1;
          } else if (b1 > a1) {
            return -1;
          } else {
            final int a2 = Integer.parseInt(aTokens.get(1));
            final int b2 = Integer.parseInt(bTokens.get(1));
            if (a2 > b2) {
              return 1;
            } else if (b2 > a2) {
              return -1;
            }
            return 0;
          }
        };
    keys.sort(sComparator);
    int differencesCount = 0;
    final List<String> onlyProfile1 = new ArrayList<>();
    final List<String> onlyProfile2 = new ArrayList<>();
    final DiffRowGenerator generator =
        DiffRowGenerator.create()
            .showInlineDiffs(true)
            .inlineDiffByWord(true)
            .oldTag(f -> "")
            .newTag(f -> "")
            .build();
    for (final String key : keys) {
      final String v1 = profile1Plan.getOrDefault(key, "").toString();
      final String v2 = profile2Plan.getOrDefault(key, "").toString();
      if (!v1.equals(v2)) {
        if (showPlanningDetail) {

          final Difference diff = new Difference();
          diff.setName("planning key '" + key + "' not equal");
          final Map<String, String> v1Map = splitTopLevelKeys(v1);
          final Map<String, String> v2Map = splitTopLevelKeys(v2);
          final List<String> keysOnlyInV1 = new ArrayList<>();
          final List<String> keysOnlyInV2 = new ArrayList<>();
          final List<String> keysThatNeedDiff = new ArrayList<>();
          for (final Map.Entry<String, String> kvp : v1Map.entrySet()) {
            final String k = kvp.getKey();
            if (!v2Map.containsKey(k)) {
              keysOnlyInV1.add(k);
            } else if (!v2Map.get(k).equals(v1Map.get(k))) {
              keysThatNeedDiff.add(k);
            }
          }
          for (final String k : v2Map.keySet()) {
            if (!v1Map.containsKey(k)) {
              keysOnlyInV2.add(k);
            }
          }
          Collections.sort(keysOnlyInV1);
          Collections.sort(keysOnlyInV2);
          Collections.sort(keysThatNeedDiff);
          final StringBuilder advice = new StringBuilder();
          if (!keysOnlyInV1.isEmpty()) {
            advice.append("only in profile1:\n");
            for (final String row : keysOnlyInV1) {
              advice.append(row);
              advice.append('\n');
            }
            advice.append("---end---\n");
          }
          if (!keysOnlyInV2.isEmpty()) {
            advice.append("only in profile2:\n");
            for (final String row : keysOnlyInV2) {
              advice.append(row);
              advice.append('\n');
            }
            advice.append("---end---\n");
          }
          if (!keysThatNeedDiff.isEmpty()) {
            advice.append("keys that have diffs\nprofile1 = <<<\nprofile2 = >>>\n\n");
            for (final String k : keysThatNeedDiff) {
              String v1Value = v1Map.get(k);
              String v2Value = v2Map.get(k);
              advice.append("key: ");
              advice.append(k);
              advice.append("\n---start---\n");
              List<String> v1Lines = Collections.singletonList(v1Value);
              if (v1Value.startsWith("{") && v1Value.endsWith("}")) {
                v1Value = v1Value.substring(1, v1Value.length() - 1);
                v1Lines = Arrays.stream(v1Value.split(",")).collect(Collectors.toList());
              }
              List<String> v2Lines = Collections.singletonList(v2Value);
              if (v2Value.startsWith("{") && v2Value.endsWith("}")) {
                v2Value = v2Value.substring(1, v2Value.length() - 1);
                v2Lines = Arrays.stream(v2Value.split(",")).collect(Collectors.toList());
              }
              // TODO we probably do not actually need this and can remove it in favor of manual
              // comparison
              final List<DiffRow> rows = generator.generateDiffRows(v1Lines, v2Lines);
              for (final DiffRow row : rows) {
                if (row.getOldLine().equals(row.getNewLine())) {
                  continue;
                }
                if (!row.getOldLine().isEmpty()) {
                  advice.append("<<<");
                  advice.append(row.getOldLine()).append('\n');
                }
                if (!"".equals(row.getNewLine())) {
                  advice.append(">>>");
                  advice.append(row.getNewLine()).append("\n\n");
                }
              }
              advice.append("---end---\n\n");
            }
          }

          diff.setProfile1Value(v1);
          diff.setProfile2Value(v2);
          diff.setAdvice(advice.toString());
          differences.add(diff);
        }

        differencesCount++;
        if (v1.isEmpty()) {
          // add to the v2 profile since v1 is empty, and they are not the same
          onlyProfile2.add(key);
        }
        if (v2.isEmpty()) {
          // add the v1 profile since v2 is empty, and they are not the same
          onlyProfile1.add(key);
        }
      }
    }
    if (differencesCount > 0) {
      final Difference diff = new Difference();
      diff.setName("found " + differencesCount + "/" + keys.size() + " plan keys differ");
      if (!showPlanningDetail) {
        diff.setAdvice("rerun command with --show-plan-details to see a diff");
      }
      onlyProfile1.sort(sComparator);
      diff.setProfile1Value("only in profile 1\n" + String.join("\n", onlyProfile1));
      onlyProfile2.sort(sComparator);
      diff.setProfile2Value("only in profile 2\n" + String.join("\n", onlyProfile2));
      differences.add(diff);
    }
    // get time in the command pool
    final double epsilon = 0.1d;
    final double diffMillis =
        Math.abs(profile1.getCommandPoolWaitMillis() - profile2.getCommandPoolWaitMillis());
    if (diffMillis > epsilon) {
      final Difference diff = new Difference();
      diff.setName("command pool time is different");
      diff.setProfile1Value(String.valueOf(profile1.getCommandPoolWaitMillis()));
      diff.setProfile2Value(String.valueOf(profile2.getCommandPoolWaitMillis()));
      String slowerProfile;
      String fasterProfile;
      if (profile1.getCommandPoolWaitMillis() > profile2.getCommandPoolWaitMillis()) {
        fasterProfile = profile2Path;
        slowerProfile = profile1Path;
      } else {
        fasterProfile = profile1Path;
        slowerProfile = profile2Path;
      }
      final String advice =
          "profile "
              + fasterProfile
              + " is "
              + diffMillis
              + " millis slower than "
              + slowerProfile
              + " which means some of the difference in performance can be accounted for by this";
      diff.setAdvice(advice);
      differences.add(diff);
    }
    // basics
    if (!Objects.equals(profile1.getDremioVersion(), profile2.getDremioVersion())) {
      final Difference diff = new Difference();
      diff.setName("dremio versions");
      diff.setProfile1Value(profile1.getDremioVersion());
      diff.setProfile2Value(profile2.getDremioVersion());
      diff.setAdvice("check known issues for each version to explain differences");
      differences.add(diff);
    }
    if (!Objects.equals(profile1.getUser(), profile2.getUser())) {
      final Difference diff = new Difference();
      diff.setName("users");
      diff.setProfile1Value(profile1.getUser());
      diff.setProfile2Value(profile2.getUser());
      diff.setAdvice("different roles can take longer to resolve see DX-25935");
      differences.add(diff);
    }
    String profile1Address = "";
    if (profile1.getForeman() != null && profile1.getForeman().getAddress() != null) {
      profile1Address = profile1.getForeman().getAddress();
    }
    String profile2Address = "";
    if (profile2.getForeman() != null && profile2.getForeman().getAddress() != null) {
      profile2Address = profile2.getForeman().getAddress();
    }
    if (!Objects.equals(profile1Address, profile2Address)) {
      final Difference diff = new Difference();
      diff.setName("coordinators");
      diff.setProfile1Value(profile1Address);
      diff.setProfile2Value(profile2Address);
      diff.setAdvice(
          "one coordinator could be slower than the other, analyze system metrics of the"
              + " coordinators and compare");
      differences.add(diff);
    }
    String profile1ClientVersion = "";
    if (profile1.getClientInfo() != null && profile1.getClientInfo().getVersion() != null) {
      profile1ClientVersion = profile1.getClientInfo().getVersion();
    }
    String profile2ClientVersion = "";
    if (profile2.getClientInfo() != null && profile2.getClientInfo().getVersion() != null) {
      profile2ClientVersion = profile2.getClientInfo().getVersion();
    }
    if (!Objects.equals(profile1ClientVersion, profile2ClientVersion)) {
      final Difference diff = new Difference();
      diff.setName("client versions");
      diff.setProfile1Value(profile1ClientVersion);
      diff.setProfile2Value(profile2ClientVersion);
      diff.setAdvice("investigate known issues with certain versions of the client");
      differences.add(diff);
    }
    String profile1ClientName = "";
    if (profile1.getClientInfo() != null && profile1.getClientInfo().getName() != null) {
      profile1ClientName = profile1.getClientInfo().getName();
    }
    String profile2ClientName = "";
    if (profile2.getClientInfo() != null && profile2.getClientInfo().getName() != null) {
      profile2ClientName = profile2.getClientInfo().getName();
    }
    if (!Objects.equals(profile1ClientName, profile2ClientName)) {
      final Difference diff = new Difference();
      diff.setName("clients");
      diff.setProfile1Value(profile1ClientName);
      diff.setProfile2Value(profile2ClientName);
      diff.setAdvice("different clients perform differently");
      differences.add(diff);
    }
    final long profile1Start = profile1.getStart();
    final long profile2Start = profile2.getStart();
    if (profile1Start != profile2Start) {
      final Difference diff = new Difference();
      diff.setName("start time");
      diff.setProfile1Value(
          Instant.ofEpochMilli(profile1Start).atZone(ZoneId.of("UTC")).toString());
      diff.setProfile2Value(
          Instant.ofEpochMilli(profile2Start).atZone(ZoneId.of("UTC")).toString());
      differences.add(diff);
    }
    final long profile1End = profile1.getEnd();
    final long profile2End = profile2.getEnd();
    if (profile1End != profile2End) {
      final Difference diff = new Difference();
      diff.setName("end time");
      diff.setProfile1Value(Instant.ofEpochMilli(profile1End).atZone(ZoneId.of("UTC")).toString());
      diff.setProfile2Value(Instant.ofEpochMilli(profile2End).atZone(ZoneId.of("UTC")).toString());
      differences.add(diff);
    }
    final double profile1Duration = profile1.getEnd() - profile1.getStart();
    final double profile2Duration = profile2.getEnd() - profile2.getStart();
    if (profile1Duration != profile2Duration) {
      final Difference diff = new Difference();
      diff.setName("duration");
      diff.setProfile1Value(getHumanDurationFromMillis((long) profile1Duration));
      diff.setProfile2Value(getHumanDurationFromMillis((long) profile2Duration));
      diff.setAdvice(
          String.format(
              "change of %.2f%%",
              ((profile2Duration - profile1Duration) / profile1Duration) * 100.0));
      differences.add(diff);
    }
    boolean profile1IsAccelerated = false;
    if (profile1.getAccelerationProfile() != null) {
      profile1IsAccelerated = profile1.getAccelerationProfile().getAccelerated();
    }
    boolean profile2IsAccelerated = false;
    if (profile2.getAccelerationProfile() != null) {
      profile2IsAccelerated = profile2.getAccelerationProfile().getAccelerated();
    }

    if (profile1IsAccelerated != profile2IsAccelerated) {
      final Difference diff = new Difference();
      diff.setName("acceleration");
      if (profile1IsAccelerated) {
        diff.setProfile1Value("accelerated");
      } else {
        diff.setProfile1Value("NOT accelerated");
      }
      if (profile2IsAccelerated) {
        diff.setProfile2Value("accelerated");
      } else {
        diff.setProfile2Value("NOT accelerated");
      }
      diff.setAdvice("not being accelerated can hurt performance");
      differences.add(diff);
    }
    long totalRecordsProfile1 = 0L;
    long totalBatchesProfile1 = 0L;
    if (profile1.getFragmentProfile() != null) {
      for (final FragmentProfile fragment : profile1.getFragmentProfile()) {
        if (fragment != null && fragment.getMinorFragmentProfile() != null) {
          for (final MinorFragmentProfile minorProfile : fragment.getMinorFragmentProfile()) {
            if (minorProfile != null && minorProfile.getOperatorProfile() != null) {
              for (final OperatorProfile operator : minorProfile.getOperatorProfile()) {
                if (operator != null && operator.getInputProfile() != null) {
                  for (final InputProfile input : operator.getInputProfile()) {
                    if (input != null) {
                      totalRecordsProfile1 += input.getRecords();
                      totalBatchesProfile1 += input.getBatches();
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    long totalBatchesProfile2 = 0;
    long totalRecordsProfile2 = 0;
    if (profile2.getFragmentProfile() != null) {
      for (final FragmentProfile fragment : profile2.getFragmentProfile()) {
        if (fragment != null && fragment.getMinorFragmentProfile() != null) {
          for (final MinorFragmentProfile minorProfile : fragment.getMinorFragmentProfile()) {
            if (minorProfile != null && minorProfile.getOperatorProfile() != null) {
              for (final OperatorProfile operator : minorProfile.getOperatorProfile()) {
                if (operator != null && operator.getInputProfile() != null) {
                  for (final InputProfile input : operator.getInputProfile()) {
                    if (input != null) {
                      totalRecordsProfile2 += input.getRecords();
                      totalBatchesProfile2 += input.getBatches();
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (totalBatchesProfile1 != totalBatchesProfile2) {
      final Difference diff = new Difference();
      diff.setName("operator batch count varies");
      diff.setProfile1Value(String.valueOf(totalBatchesProfile1));
      diff.setProfile2Value(String.valueOf(totalBatchesProfile2));
      diff.setAdvice(
          String.format(
              "change of %.2f%%",
              ((totalBatchesProfile2 - totalBatchesProfile1) / (double) totalBatchesProfile1)
                  * 100.0));
      differences.add(diff);
    }
    if (totalRecordsProfile1 != totalRecordsProfile2) {
      final Difference diff = new Difference();
      diff.setName("operator record count varies");
      diff.setProfile1Value(String.valueOf(totalRecordsProfile1));
      diff.setProfile2Value(String.valueOf(totalRecordsProfile2));
      diff.setAdvice(
          String.format(
              "change of %.2f%%",
              ((totalRecordsProfile2 - totalRecordsProfile1) / (double) totalRecordsProfile1)
                  * 100.0));
      differences.add(diff);
    }
    if (!(profile1.getState() == profile2.getState())) {
      final Difference diff = new Difference();
      diff.setName("query state");
      final QueryState profile1QueryState = QueryState.values()[profile1.getState()];
      diff.setProfile1Value(profile1QueryState.toString());
      final QueryState profile2QueryState = QueryState.values()[profile2.getState()];
      diff.setProfile2Value(profile2QueryState.toString());
      diff.setAdvice("");
      differences.add(diff);
    }
    String profile1Queue = "";
    if (profile1.getResourceSchedulingProfile() != null
        && profile1.getResourceSchedulingProfile().getQueueName() != null) {
      profile1Queue = profile1.getResourceSchedulingProfile().getQueueName();
    }
    String profile2Queue = "";
    if (profile2.getResourceSchedulingProfile() != null
        && profile2.getResourceSchedulingProfile().getQueueName() != null) {
      profile2Queue = profile2.getResourceSchedulingProfile().getQueueName();
    }
    double profile1Cost = 0.0;
    if (profile1.getResourceSchedulingProfile() != null
        && profile1.getResourceSchedulingProfile().getSchedulingProperties() != null) {
      profile1Cost =
          profile1.getResourceSchedulingProfile().getSchedulingProperties().getQueryCost();
    }
    double profile2Cost = 0.0;
    if (profile2.getResourceSchedulingProfile() != null
        && profile2.getResourceSchedulingProfile().getSchedulingProperties() != null) {
      profile2Cost =
          profile2.getResourceSchedulingProfile().getSchedulingProperties().getQueryCost();
    }

    if (!Objects.equals(profile1Queue, profile2Queue)) {
      final Difference diff = new Difference();
      diff.setName("query queue");
      diff.setProfile1Value(profile1Queue);
      diff.setProfile2Value(profile2Queue);
      diff.setAdvice("");
      differences.add(diff);
    }
    if (Math.abs(profile2Cost - profile1Cost) > 0.1) {
      final Difference diff = new Difference();
      diff.setName("query cost");
      diff.setProfile1Value(String.format("%,.2f", profile1Cost));
      diff.setProfile2Value(String.format("%,.2f", profile2Cost));
      diff.setAdvice(
          String.format("change of %.2f%%", (profile2Cost - profile1Cost) / profile1Cost * 100.0));
      differences.add(diff);
    }

    // slowest of all operators
    final Operator profile1SlowOperator =
        findSlowestOperator(profile1, ProfileDifferenceReport::noOpOperator);
    final Operator profile2SlowOperator =
        findSlowestOperator(profile2, ProfileDifferenceReport::noOpOperator);
    if (!Objects.equals(profile1SlowOperator, profile2SlowOperator)) {
      final Difference diff = new Difference();
      diff.setName("slowest operator");
      if (profile1SlowOperator != null) {
        diff.setProfile1Value(profile1SlowOperator.toString());
      } else {
        diff.setProfile1Value("");
      }
      if (profile2SlowOperator != null) {
        diff.setProfile2Value(profile2SlowOperator.toString());
      } else {
        diff.setProfile2Value("");
      }
      diff.setAdvice("on further digging this may reveal the core issue");
      differences.add(diff);
    }
    // compare the slowest scan
    final Operator profile1SlowScanOperator =
        findSlowestOperator(profile1, ProfileDifferenceReport::scanOperators);
    final Operator profile2SlowScanOperator =
        findSlowestOperator(profile2, ProfileDifferenceReport::scanOperators);
    if (!Objects.equals(profile1SlowOperator, profile1SlowScanOperator)
        && !Objects.equals(profile2SlowOperator, profile2SlowScanOperator)) {
      if (!Objects.equals(profile1SlowScanOperator, profile2SlowScanOperator)) {
        final Difference diff = new Difference();
        diff.setName("slowest scan operator");
        if (profile1SlowScanOperator != null) {
          diff.setProfile1Value(profile1SlowScanOperator.toString());
        } else {
          diff.setProfile1Value("");
        }
        if (profile2SlowScanOperator != null) {
          diff.setProfile2Value(profile2SlowScanOperator.toString());
        } else {
          diff.setProfile2Value("");
        }
        diff.setAdvice("on further digging this may reveal the core issue");
        differences.add(diff);
      }
    }
    return differences;
  }
}
