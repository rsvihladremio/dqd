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

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;
import static java.util.Arrays.asList;

import com.dremio.support.diagnostics.queriesjson.Query;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

public class MaxCPUTimeWriter {
  public static String generate(final Collection<Query> top) {
    final StringBuilder builder = new StringBuilder();
    if (top.isEmpty()) {
      builder.append("<h2>Max Excecution CPU Time</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    var htmlBuilder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    top.forEach(
        x ->
            rows.add(
                asList(
                    col(x.getQueryId()),
                    col(Dates.format(Instant.ofEpochMilli(x.getStart())), x.getStart()),
                    col(
                        Human.getHumanDurationFromMillis(x.getFinish() - x.getStart()),
                        x.getFinish() - x.getStart()),
                    col(
                        Human.getHumanDurationFromMillis(x.getExecutionCpuTimeNs() / 1_000_000),
                        x.getMemoryAllocated()),
                    col(x.getQueryText(), true))));

    builder.append(
        htmlBuilder.generateTable(
            "maxCPUTimeTable",
            "Max Excecution CPU Time",
            asList("query id", "start", "query duration", "cpu time", "query"),
            rows));
    return builder.toString();
  }
}
