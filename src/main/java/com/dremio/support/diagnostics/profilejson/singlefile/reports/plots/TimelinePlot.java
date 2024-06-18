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
package com.dremio.support.diagnostics.profilejson.singlefile.reports.plots;

public class TimelinePlot {

  private final CandleStickWriter candleStickWriter = new CandleStickWriter();

  /**
   * generates a chart with stop and end times of each phase, this is useful for seeing a timeline
   * and seeing at a glance at a given time which phases are running and how long they take.
   *
   * @param phaseThreadNames names of threads to display
   * @param startTimes array of start times in epoch milliseconds
   * @param endTimes array of end times in epoch milliseconds
   * @param phaseThreadTextNames used on mouseover in the timeline
   */
  public String generatePlot(
      final String[] phaseThreadNames,
      final long[] startTimes,
      final long[] endTimes,
      final String... phaseThreadTextNames) {
    return candleStickWriter.candleStick(
        "phaseThreadTimeline",
        "Phases",
        "Thread Timeline",
        phaseThreadNames,
        startTimes,
        endTimes,
        phaseThreadTextNames);
  }
}
