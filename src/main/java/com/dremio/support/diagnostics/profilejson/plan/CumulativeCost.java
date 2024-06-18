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

public class CumulativeCost {
  private final double rows;
  private final double cpu;
  private final double io;
  private final double network;
  private final double memory;

  /**
   * Represents the relative cost of a query or a planning phase
   *
   * @param rows row cost
   * @param cpu cpu cost
   * @param io io cost
   * @param network network cost
   * @param memory memory cost
   */
  public CumulativeCost(
      final double rows,
      final double cpu,
      final double io,
      final double network,
      final double memory) {
    this.rows = rows;
    this.cpu = cpu;
    this.io = io;
    this.network = network;
    this.memory = memory;
  }

  /**
   * the cost cause by the rows estimated in the planner
   *
   * @return cost of the rows returned
   */
  public double getRows() {
    return rows;
  }

  /**
   * the estimated cost to the CPU that this operation will entail
   *
   * @return cost to the CPU
   */
  public double getCpu() {
    return cpu;
  }

  /**
   * the estimated cost to the IO that this operation will entail
   *
   * @return cost to the IO layer
   */
  public double getIo() {
    return io;
  }

  /**
   * the estimated cost to the network that this operation will entail
   *
   * @return cost to the network layer
   */
  public double getNetwork() {
    return network;
  }

  /**
   * The estimated cost to the RAM subsystem that this operation will entail
   *
   * @return cost to RAM
   */
  public double getMemory() {
    return memory;
  }
}
