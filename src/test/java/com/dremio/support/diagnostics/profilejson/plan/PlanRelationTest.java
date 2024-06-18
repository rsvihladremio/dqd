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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class PlanRelationTest {

  @Test
  void testPlanRelationGettersTest() {
    CumulativeCost upCost = new CumulativeCost(10.0, 20.0, 30.0, 40.0, 50.0);
    Map<String, Object> upValues = new HashMap<>();
    upValues.put("fetch", 1L);
    PlanRelation upstream =
        new PlanRelation(
            "myUpstream",
            "myUpstreamOp",
            upValues,
            new ArrayList<>(),
            new ArrayList<>(),
            100,
            upCost);

    CumulativeCost cost = new CumulativeCost(10.0, 20.0, 30.0, 40.0, 50.0);
    Map<String, Object> values = new HashMap<>();
    values.put("fetch", 1L);
    // setup main relation
    PlanRelation relation =
        new PlanRelation("myName", "myOp", values, new ArrayList<>(), new ArrayList<>(), 500, cost);

    // downstream
    CumulativeCost downCost = new CumulativeCost(10.0, 20.0, 30.0, 40.0, 50.0);
    Map<String, Object> downValues = new HashMap<>();
    values.put("split", 10L);
    PlanRelation downstream =
        new PlanRelation(
            "myDownstream",
            "MyDownstreamOp",
            downValues,
            new ArrayList<>(),
            new ArrayList<>(),
            1000,
            downCost);

    // setup relationships
    relation.getUpstream().add(upstream);
    upstream.getDownstream().add(relation);
    relation.getDownstream().add(downstream);
    downstream.getUpstream().add(relation);

    assertEquals(relation.getCumulativeCost(), cost);
    assertEquals(relation.getName(), "myName");
    assertEquals(relation.getOp(), "myOp");
    assertEquals(relation.getRowCount(), 500.0);
    assertEquals(relation.getValues(), values);
    assertEquals(relation.getDownstream().get(0), downstream);
    assertEquals(relation.getUpstream().get(0), upstream);
  }
}
