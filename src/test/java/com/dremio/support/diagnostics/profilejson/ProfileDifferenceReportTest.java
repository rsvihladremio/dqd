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

import static com.dremio.support.diagnostics.FileTestHelpers.getTestProfile1;
import static com.dremio.support.diagnostics.FileTestHelpers.getTestProfile2;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProfileDifferenceReportTest {

  @Test
  void splitTopLevelKeys() {
    String plan =
        "{\"op\"=com.dremio.exec.planner.physical.ScreenPrel, \"values\"={}, \"inputs\"=[00-01],"
            + " \"rowType\"=RecordType(VARCHAR(65536) Fragment, BIGINT Records, VARCHAR(65536)"
            + " Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT FileSize,"
            + " VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema, VARBINARY(65536)"
            + " ARRAY PartitionData), \"rowCount\"=1004.7199999999997,"
            + " \"cumulativeCost\"={20988.072000000007 rows, 409286.3672967999 cpu, 132200.0 io,"
            + " 132200.0 network, 139603.2 memory}}";
    Map<String, String> keys = ProfileDifferenceReport.splitTopLevelKeys(plan);
    assertThat(keys.keySet().size()).isEqualTo(6);
    assertThat(keys.keySet().toArray()).contains("\"rowCount\"");
    assertThat(keys.keySet().toArray()).contains("\"rowType\"");
    assertThat(keys.keySet().toArray()).contains("\"inputs\"");
    assertThat(keys.keySet().toArray()).contains("\"values\"");
    assertThat(keys.keySet().toArray()).contains("\"op\"");
    assertThat(keys.keySet().toArray()).contains("\"cumulativeCost\"");
  }

  @Test
  void testDifferences() throws IOException {
    Locale.setDefault(new Locale("en", "US"));
    final ProfileDifferenceReport report = new ProfileDifferenceReport();
    final ProfileJSONParser parser = new ProfileJSONParser();
    final ProfileJSON parsed1 = parser.parseFile(getTestProfile1().stream());
    final ProfileJSON parsed2 = parser.parseFile(getTestProfile2().stream());
    final boolean showPlanningDetail = false;
    List<Difference> differences =
        report.getDifferences(
            getTestProfile1().filePath().toString(),
            getTestProfile2().filePath().toString(),
            showPlanningDetail,
            parsed1,
            parsed2);
    assertThat(differences.size()).isEqualTo(7);
    Difference diffPlan = new Difference();
    diffPlan.setName("found 9/9 plan keys differ");
    diffPlan.setAdvice("rerun command with --show-plan-details to see a diff");
    diffPlan.setProfile1Value("only in profile 1\n");
    diffPlan.setProfile2Value("only in profile 2\n");
    assertThat(differences.get(0)).isEqualTo(diffPlan);

    Difference diffStart = new Difference();
    diffStart.setName("start time");
    diffStart.setProfile1Value("2022-10-13T11:12:56.641Z[UTC]");
    diffStart.setProfile2Value("2022-10-13T11:23:51.270Z[UTC]");
    assertThat(differences.get(1)).isEqualTo(diffStart);

    Difference diffEnd = new Difference();
    diffEnd.setName("end time");
    diffEnd.setProfile1Value("2022-10-13T11:12:59.063Z[UTC]");
    diffEnd.setProfile2Value("2022-10-13T11:23:54.084Z[UTC]");
    assertThat(differences.get(2)).isEqualTo(diffEnd);

    Difference diffDuration = new Difference();
    diffDuration.setName("duration");
    diffDuration.setProfile1Value("2.42 seconds");
    diffDuration.setProfile2Value("2.81 seconds");
    diffDuration.setAdvice("change of 16.18%");
    assertThat(differences.get(3)).isEqualTo(diffDuration);

    Difference diffBatchCount = new Difference();
    diffBatchCount.setName("operator batch count varies");
    diffBatchCount.setAdvice("change of -67.74%");
    diffBatchCount.setProfile1Value("31");
    diffBatchCount.setProfile2Value("10");
    assertThat(differences.get(4)).isEqualTo(diffBatchCount);

    Difference diffRecordCount = new Difference();
    diffRecordCount.setName("operator record count varies");
    diffRecordCount.setAdvice("change of -49.85%");
    diffRecordCount.setProfile1Value("2913");
    diffRecordCount.setProfile2Value("1461");
    assertThat(differences.get(5)).isEqualTo(diffRecordCount);

    assertThat(differences.get(6).getName()).isEqualTo("slowest operator");
    assertThat(differences.get(6).getAdvice())
        .isEqualTo("on further digging this may reveal the core issue");
    assertThat(differences.get(6).getProfile1Value())
        .contains("TEXT_SUB_SCAN")
        .contains("phase-thread=0-0");
    assertThat(differences.get(6).getProfile2Value())
        .contains("TEXT_SUB_SCAN")
        .contains("phase-thread=0-0");
  }

  @Test
  void testDifferencesInStartAndEndTime() {
    Locale.setDefault(Locale.US);

    // same time
    final ProfileDifferenceReport report = new ProfileDifferenceReport();
    ProfileJSON profile1 = new ProfileJSON();
    profile1.setStart(1L);
    profile1.setEnd(100L);
    ProfileJSON profile2 = new ProfileJSON();
    profile2.setStart(1L);
    profile2.setEnd(100L);
    List<Difference> diffs =
        report.getDifferences("profile1", "profile2", false, profile1, profile2);
    assertThat(diffs.size()).isEqualTo(0);
    // start is different
    profile1.setStart(10L);
    profile2.setStart(11L);
    diffs = report.getDifferences("profile1", "profile2", false, profile1, profile2);
    assertThat(diffs.size()).isEqualTo(2);
    assertThat(diffs.get(0).getName()).isEqualTo("start time");
    assertThat(diffs.get(0).getProfile1Value()).isEqualTo("1970-01-01T00:00:00.010Z[UTC]");
    assertThat(diffs.get(0).getProfile2Value()).isEqualTo("1970-01-01T00:00:00.011Z[UTC]");
    assertThat(diffs.get(0).getAdvice()).isEqualTo(null);
    assertThat(diffs.get(1).getName()).isEqualTo("duration");
    assertThat(diffs.get(1).getProfile1Value()).isEqualTo("90 millis");
    assertThat(diffs.get(1).getProfile2Value()).isEqualTo("89 millis");
    assertThat(diffs.get(1).getAdvice()).isEqualTo("change of -1.11%");
    // end is different
    profile1.setStart(10L);
    profile2.setStart(10L);
    profile1.setEnd(100L);
    profile2.setEnd(110L);
    diffs = report.getDifferences("profile1", "profile2", false, profile1, profile2);
    assertThat(diffs.size()).isEqualTo(2);
    assertThat(diffs.get(0).getName()).isEqualTo("end time");
    assertThat(diffs.get(0).getProfile1Value()).isEqualTo("1970-01-01T00:00:00.100Z[UTC]");
    assertThat(diffs.get(0).getProfile2Value()).isEqualTo("1970-01-01T00:00:00.110Z[UTC]");
    assertThat(diffs.get(0).getAdvice()).isEqualTo(null);
  }

  @Test
  void testDifferencesSameFile() throws IOException {
    final ProfileDifferenceReport report = new ProfileDifferenceReport();
    final ProfileJSONParser parser = new ProfileJSONParser();
    final ProfileJSON parsed1 = parser.parseFile(getTestProfile1().stream());
    final ProfileJSON parsed2 = parser.parseFile(getTestProfile1().stream());
    List<Difference> differences =
        report.getDifferences(
            getTestProfile1().filePath().toString(),
            getTestProfile1().filePath().toString(),
            true,
            parsed1,
            parsed2);
    System.out.println(differences);
    assertThat(differences.size()).isEqualTo(0);
  }
}
