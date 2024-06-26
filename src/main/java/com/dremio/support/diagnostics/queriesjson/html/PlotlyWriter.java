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
package com.dremio.support.diagnostics.queriesjson.html;

import java.time.Instant;
import java.util.function.Function;

public class PlotlyWriter {

  /**
   * @param traceId html to use for the generated html element
   * @param title trace title to use
   * @param dp datapoints to populate the
   * @return returns a plotly trace for the dates and data generated
   */
  public String writeTraceHtml(
      final String traceId,
      final String title,
      Dates.BucketIterator iterTs,
      Function<Long, String> gen) {
    final var xStr = new StringBuilder();
    final var yStr = new StringBuilder();
    boolean isFirst = true;
    while (iterTs.hasNext()) {
      if (isFirst) {
        isFirst = false;
      } else {
        xStr.append(",");
        yStr.append(",");
      }
      var ts = iterTs.next();
      xStr.append("\"%s\"".formatted(Instant.ofEpochMilli(ts).toString()));
      yStr.append(gen.apply(ts));
    }
    return "var "
        + traceId
        + " = {x:["
        + xStr
        + "],y:["
        + yStr
        + "],mode: 'lines',"
        + "xaxis: 'x',"
        + "yaxis: 'y',"
        + "type: 'scatter',"
        + "name: '"
        + title
        + "'};";
  }

  public String writePlotHtml(
      final String title, final String plotId, final String[] traceIds, final String... traces) {
    final StringBuilder builder = new StringBuilder();
    builder.append("\n<div id='");
    builder.append(plotId);
    builder.append("' />");
    builder.append("<script type=\"text/javascript\">\n");
    builder.append("var ");
    builder.append(plotId);
    builder.append(" = document.getElementById('");
    builder.append(plotId);
    builder.append("');\n");
    for (final String trace : traces) {
      builder.append(trace);
      builder.append("\n");
    }
    builder.append("Plotly.newPlot(");
    builder.append(plotId);
    builder.append(",");
    builder.append("[");
    builder.append(String.join(", ", traceIds));
    builder.append("],");
    builder.append("{title: '");
    builder.append(title);
    builder.append("',");
    builder.append("height: 450,");
    builder.append("width: 1280});\n");
    builder.append("</script>");
    return builder.toString();
  }
}
