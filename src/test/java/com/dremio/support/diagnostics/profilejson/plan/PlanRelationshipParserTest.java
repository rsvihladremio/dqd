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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.profilejson.ProfileJSONParser;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PlanRelationshipParserTest {

  @Test
  void testParseWithCumulativeCostTiny() {
    PlanRelationshipParser parser = new PlanRelationshipParser();
    ProfileJSON profile = new ProfileJSON();
    profile.setJsonPlan(
        "{\"09-10\": {\n"
            + "    \"op\": \"com.dremio.exec.planner.physical.EmptyPrel\",\n"
            + "    \"values\": {\n"
            + "      \"schema\": \"schema(DATASET_ID::int32, IS_LATEST::int32, dir0::varchar)\"\n"
            + "    },\n"
            + "    \"inputs\": [],\n"
            + "    \"rowType\": \"RecordType(INTEGER DATASET_ID, INTEGER IS_LATEST, VARCHAR(65536)"
            + " dir0)\",\n"
            + "    \"rowCount\": 1,\n"
            + "    \"cumulativeCost\": \"{tiny}\"\n"
            + "  }}");
    List<PlanRelation> planRelations = parser.getPlanRelations(profile);
    assertEquals(
        0.0d,
        planRelations.get(0).getCumulativeCost().getCpu(),
        0.01,
        "cumulative cost should be empty");
    assertEquals(
        0.0d,
        planRelations.get(0).getCumulativeCost().getIo(),
        0.01,
        "cumulative cost should be empty");
    assertEquals(
        0.0d,
        planRelations.get(0).getCumulativeCost().getMemory(),
        0.01,
        "cumulative cost should be empty");
    assertEquals(
        0.0d,
        planRelations.get(0).getCumulativeCost().getRows(),
        0.01,
        "cumulative cost should be empty");
    assertEquals(
        0.0d,
        planRelations.get(0).getCumulativeCost().getNetwork(),
        0.01,
        "cumulative cost should be empty");
  }

  @Test
  void testParseGetPlanRelationship() throws IOException {
    PlanRelationshipParser parser = new PlanRelationshipParser();
    ProfileJSON profile;
    try (InputStream stream = FileTestHelpers.getTestProfile1().stream()) {
      profile = new ProfileJSONParser().parseFile(stream);
    }
    List<PlanRelation> planRelations = parser.getPlanRelations(profile);
    // should only have one parent
    assertEquals(9, planRelations.size());
    PlanRelation parent = planRelations.get(0);
    assertEquals("00-00", parent.getName());
    assertEquals("com.dremio.exec.planner.physical.ScreenPrel", parent.getOp());
    assertEquals(1004.72, parent.getRowCount(), 0.01);
    PlanRelation second = planRelations.get(1);
    assertEquals("00-01", second.getName());
    PlanRelation third = planRelations.get(2);
    assertEquals("00-02", third.getName());
    PlanRelation fourth = planRelations.get(3);
    assertEquals("00-03", fourth.getName());
    PlanRelation fifth = planRelations.get(4);
    assertEquals("00-04", fifth.getName());
    PlanRelation sixth = planRelations.get(5);
    assertEquals("00-05", sixth.getName());
    PlanRelation seventh = planRelations.get(6);
    assertEquals("00-06", seventh.getName());
    PlanRelation eighth = planRelations.get(7);
    assertEquals("00-07", eighth.getName());
    PlanRelation ninth = planRelations.get(8);
    assertEquals("00-08", ninth.getName());
    CumulativeCost cumulativeCost = parent.getCumulativeCost();

    assertEquals(20988.07, cumulativeCost.getRows(), 0.01);
    assertEquals(409286.36, cumulativeCost.getCpu(), 0.01);
    assertEquals(132200.0, cumulativeCost.getIo(), 0.01);
    assertEquals(132200.2, cumulativeCost.getNetwork(), 0.01);
    assertEquals(139603.2, cumulativeCost.getMemory(), 0.01);
  }
}
