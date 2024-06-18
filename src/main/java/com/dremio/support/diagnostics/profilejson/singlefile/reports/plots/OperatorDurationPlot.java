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

public class OperatorDurationPlot {
  /**
   * generate plot for duration for each operator
   *
   * @param operatorNames the list of operators
   * @param operatorTimes how long in milliseconds did each operator last
   * @param operatorText list for hover text for operators
   */
  public String generatePlot(
      final String[] operatorNames, final long[] operatorTimes, final String... operatorText) {
    final String operatorTrace =
        ProfileTraceWriter.writeTrace(
            "operatorTrace",
            "operator duration",
            operatorNames,
            operatorTimes,
            TraceType.SCATTER,
            operatorText);
    return PlotWriter.writePlot(
        "Operator Duration by Phase",
        "Phases",
        "Duration Milliseconds (setup+process+wait time)",
        "operatorsDuration",
        new String[] {"operatorTrace"},
        new String[] {operatorTrace},
        50);
  }
}
