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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class PlanNodeTest {

  @Test
  void testPlanNodeGettersSetters() {
    PlanNode node = new PlanNode();
    String cost =
        "CumulativeCost:{rows: 200.0, cpu: 30.0, io: 400.0, network: 500.0, memory: 600.0}";
    node.setCumulativeCost(cost);
    List<String> inputs = new ArrayList<>();
    inputs.add("adbc");
    node.setInputs(inputs);
    String op = "LogicalSort";
    node.setOp(op);
    double rowCount = 1000.0;
    node.setRowCount(rowCount);
    Map<String, Object> props = new HashMap<>();
    props.put("ping", "pong");
    node.setValues(props);
  }
}
