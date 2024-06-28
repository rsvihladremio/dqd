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
package com.dremio.support.diagnostics.shared;

import java.util.Collection;

public class HtmlTableBuilder {
  public <D, S> String generateTable(
      final String tableID,
      final String caption,
      final Collection<String> headers,
      final Collection<Collection<HtmlTableDataColumn<D, S>>> rows) {
    StringBuilder builder = new StringBuilder();

    builder.append("<h3>");
    builder.append(caption);
    builder.append("</h3>\n");
    if (rows != null && !rows.isEmpty()) {
      builder.append("<button onClick=\"exportAsCSV('");
      builder.append(tableID);
      builder.append("')\")>EXPORT</button>\n");
    }
    final var inputID = tableID + "Input";
    final var spanID = tableID + "Span";
    builder.append("<label>filter: </label>");
    builder.append("<input id=\"");
    builder.append(inputID);
    builder.append("\" type=\"text\" onkeyup=\"filterTable('");
    builder.append(tableID);
    builder.append("', '");
    builder.append(inputID);
    builder.append("', '");
    builder.append(spanID);
    builder.append("')\"</>");
    builder.append("<span id=\"");
    builder.append(spanID);
    builder.append("\">");
    String name = "row";
    int rowCount = 0;
    if (rows != null) {
      rowCount = rows.size();
    }
    if (rowCount != 1) {
      name += "s";
    }
    String rowTitle = " %d %s shown".formatted(rowCount, name);
    builder.append(rowTitle);
    builder.append("</span>");
    builder.append("<table class=\"sortable\" id=\"");
    builder.append(tableID);
    builder.append("\">\n");
    builder.append("<thead>\n");
    for (final String header : headers) {
      builder.append("<th>");
      builder.append(header);
      builder.append("</th>\n");
    }
    builder.append("</thead>\n");
    builder.append("<tbody>\n");
    if (rows != null) {
      for (final Collection<HtmlTableDataColumn<D, S>> detail : rows) {
        builder.append("<tr>");
        for (final HtmlTableDataColumn<D, S> column : detail) {
          String classes = "";
          if (column.limitText()) {
            classes = " class=\"tooltip-pr\"";
          }
          if (column.sortableData() != null) {
            // the data-sort attribute works with https://tofsjonas.github.io/sortable/ to use a
            // machine parseable sort column
            builder.append("<td data-sort=\"");
            builder.append(column.sortableData());
            builder.append("\"%s>".formatted(classes));
          } else {
            builder.append("<td%s>".formatted(classes));
          }
          if (column.limitText()) {
            builder.append("<span class=\"tooltiptext-pr\">");
            builder.append(column.data());
            builder.append("</span>");
          } else {
            builder.append(column.data());
          }
          builder.append("</td>\n");
        }
        builder.append("</tr>\n");
      }
    }
    builder.append("</tbody>\n");
    builder.append("</table>\n");
    return builder.toString();
  }
}
