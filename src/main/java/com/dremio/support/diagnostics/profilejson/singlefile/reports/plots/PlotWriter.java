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

public class PlotWriter {
  /**
   * general purpose plot writer that takes a series of traces and their ids.
   *
   * @param title title of the plot
   * @param yAxisTitle describes the y-axis
   * @param xAxisTitle describes the x-axis
   * @param elementId html element id which should be unique the page used
   * @param traceIds list of javascript variable names for the traces, they are indexed on the same
   *     level
   * @param traces list of traces to reference
   * @param labelMargin pixels to give some whitespace around the labels
   * @param layoutFields optional extra parameters to send the layout, use this to customize the
   *     plot
   */
  public static String writePlot(
      final String title,
      final String yAxisTitle,
      final String xAxisTitle,
      final String elementId,
      final String[] traceIds,
      final String[] traces,
      final int labelMargin,
      final String... layoutFields) {
    final StringBuilder builder = new StringBuilder();
    builder.append("<div id='");
    builder.append(elementId);
    builder.append("'/>");
    builder.append("<script type=\"text/javascript\">\n");
    builder.append("var ");
    builder.append(elementId);
    builder.append(" = document.getElementById('");
    builder.append(elementId);
    builder.append("');\n");
    for (final String trace : traces) {
      builder.append(trace);
    }
    builder.append("\nvar data = [");
    builder.append(String.join(", ", traceIds));
    builder.append("];");
    builder.append(
        String.format(
            "\n"
                + "var layout = { "
                + "  margin: { l: "
                + labelMargin
                + String.format(
                    " }, yaxis: { title: '%s', type: 'category'}, xaxis: { title: '%s', tickangle:",
                    yAxisTitle, xAxisTitle)
                + " -60 }, title: '"));
    builder.append(title);
    builder.append("'");
    if (layoutFields.length > 0) {
      for (final String f : layoutFields) {
        builder.append(", ");
        builder.append(f);
      }
    }
    builder.append("};");
    builder.append("\nPlotly.newPlot(");
    builder.append(elementId);
    builder.append(",data , layout)");
    builder.append(";\n</script>");

    return builder.toString();
  }
}
