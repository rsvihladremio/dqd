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

public class TraceData {
  private final String[] phaseThreadNames;
  private final String[] phaseThreadTextNames;
  private final long[] phaseProcessTimes;
  private final long[] startTimes;
  private final long[] endTimes;

  public TraceData(
      final String[] phaseThreadNames,
      final String[] phaseThreadTextNames,
      final long[] phaseProcessTimes,
      final long[] startTimes,
      final long[] endTimes) {
    this.phaseThreadNames = phaseThreadNames;
    this.phaseThreadTextNames = phaseThreadTextNames;
    this.phaseProcessTimes = phaseProcessTimes;
    this.startTimes = startTimes;
    this.endTimes = endTimes;
  }

  public String[] getPhaseThreadNames() {
    return phaseThreadNames;
  }

  public String[] getPhaseThreadTextNames() {
    return phaseThreadTextNames;
  }

  public long[] getPhaseProcessTimes() {
    return phaseProcessTimes;
  }

  public long[] getEndTimes() {
    return endTimes;
  }

  public long[] getStartTimes() {
    return startTimes;
  }
}
