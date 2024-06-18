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

import java.util.Locale;

public class ProfileTraceWriter {

  /**
   * generates a trace for any number of charts and plot and is fundamental to the graphing
   * capability of DQD, while atm this is aimed at only profile tracing, it can be generalized into
   * a method used by every charting need.
   *
   * @param traceId html variable name (useful for avoiding collisions)
   * @param title visible title of the trace, will show up in the legend in the charts
   * @param x x axis data for trace
   * @param y y axis data for trace
   * @param traceType the type of chart the trace will generate
   * @param textLabels mouse over text for the data points
   */
  public static String writeTrace(
      final String traceId,
      final String title,
      final String[] x,
      final long[] y,
      final TraceType traceType,
      final String... textLabels) {
    final StringBuilder builder = new StringBuilder();
    builder.append("var ");
    builder.append(traceId);
    builder.append("= { y: [");
    for (final String e : x) {
      builder.append("\"");
      builder.append(e);
      builder.append("\",");
    }
    builder.append("],");
    builder.append("\nx: [");
    for (final long e : y) {
      builder.append(e);
      builder.append(",");
    }
    builder.append("], ");
    // if there are textLables go ahead and add them
    if (textLabels.length > 0) {
      builder.append("text: [");
      for (final String e : textLabels) {
        builder.append("\"");
        builder.append(e);
        builder.append("\",");
      }
      builder.append("], ");
    }
    builder.append(" orientation: 'h', mode: '" + "markers" + "', xaxis: 'x', yaxis: 'y', type: '");
    builder.append(traceType.toString().toLowerCase(Locale.US));
    builder.append("', name:'");
    builder.append(title);
    builder.append("',};");
    return builder.toString();
  }
}
