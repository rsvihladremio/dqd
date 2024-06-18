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

import java.time.Instant;

public class CandleStickWriter {

  /**
   * generic writer of plots with start and end times tied together, where one sees the start and
   * end time of a given element, this is useful for plotting things that have a duration and that
   * duration is relevant
   *
   * @param elementId HTML element id to prevent collisions of more than one candleStick
   * @param yTitle title of the plot in the y-axis
   * @param xTitle title of the plot in the x-axis
   * @param texts display text
   * @param startTimes an array of start times in epoch miliseconds
   * @param endTimes an array of start times in epoch milliseconds
   * @param hoverTexts the mouse over text
   */
  public String candleStick(
      final String elementId,
      final String yTitle,
      final String xTitle,
      final String[] texts,
      final long[] startTimes,
      final long[] endTimes,
      final String... hoverTexts) {
    final boolean allLengthsMatch =
        (startTimes.length == endTimes.length) && (endTimes.length == texts.length);
    if (!allLengthsMatch) {
      throw new RuntimeException(
          "critical bug in generating start/stop chart we cannot have a different number of start"
              + " and stop times");
    }
    final StringBuilder data = new StringBuilder();
    for (int i = 0; i < startTimes.length; i++) {
      final long startTime = startTimes[i];
      final long endTime = endTimes[i];
      final String text = texts[i];
      final String label = hoverTexts[i];
      data.append("data.push({type: 'scatter',x: [\"")
          .append(Instant.ofEpochMilli(startTime).toString())
          .append("\",\"")
          .append(Instant.ofEpochMilli(endTime).toString())
          .append("\"],")
          .append("y: [\"")
          .append(text)
          .append("\",\"")
          .append(text)
          .append("\"],")
          .append("text: [\"")
          .append("START ")
          .append(label)
          .append("\",\"")
          .append("END ")
          .append(label)
          .append("\"],")
          .append("mode: 'lines+markers',")
          .append("}")
          .append(");\n");
    }
    final String divStyle = "<div id=\"" + elementId + "\"/>\n";
    final String plotlySave = "Plotly.newPlot('" + elementId + "', data, layout);";
    return divStyle
        + "<script>\n"
        + "var data = [];"
        + data
        + "layout = { "
        + "yaxis: { title: '"
        + yTitle
        + "', type: 'category', categoryorder: 'category ascending'},"
        + "title: '"
        + xTitle
        + "',"
        + "showlegend: false"
        + "}\n;"
        + plotlySave
        + "</script>";
  }
}
