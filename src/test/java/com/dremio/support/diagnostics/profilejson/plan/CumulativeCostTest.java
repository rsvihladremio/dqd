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

import org.junit.jupiter.api.Test;

public class CumulativeCostTest {

  @Test
  void testCumulativeCostGetters() {
    double rows = 200.0;
    double cpu = 300.0;
    double io = 400.0;
    double network = 500.0;
    double memory = 600.0;
    CumulativeCost cost = new CumulativeCost(rows, cpu, io, network, memory);
    assertEquals(rows, cost.getRows(), 0.01);
    assertEquals(cpu, cost.getCpu(), 0.01);
    assertEquals(io, cost.getIo(), 0.01);
    assertEquals(network, cost.getNetwork(), 0.01);
    assertEquals(memory, cost.getMemory(), 0.01);
  }
}
