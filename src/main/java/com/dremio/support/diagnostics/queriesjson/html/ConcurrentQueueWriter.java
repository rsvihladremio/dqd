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

import com.dremio.support.diagnostics.queriesjson.Strings;
import com.dremio.support.diagnostics.shared.Human;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;

public class ConcurrentQueueWriter {
  private final PlotlyWriter plotly = new PlotlyWriter();
  private long window;

  public ConcurrentQueueWriter(long window) {
    this.window = window;
  }

  public String generate(
      long start,
      long end,
      Map<String, Map<Long, Long>> queueCounts,
      final Map<Long, Long> schemaOpsCounts,
      final Map<Long, Long> totalQueryCounts) {
    var traceIds = new ArrayList<>();
    var traces = new ArrayList<>();
    var totalQueries =
        plotly.writeTraceHtml(
            "allQueries",
            "all queries",
            new Dates.BucketIterator(start, end, this.window),
            (b) -> {
              if (totalQueryCounts.containsKey(b)) {
                return totalQueryCounts.get(b).toString();
              } else {
                return "0";
              }
            });
    traces.add(totalQueries);
    traceIds.add("allQueries");

    for (var entry : queueCounts.entrySet()) {
      var queueName = entry.getKey();
      var map = entry.getValue();
      var traceNameId = String.format("queueName%s", Strings.escape(queueName));
      Function<Long, String> gen =
          (bucket) -> {
            if (map.containsKey(bucket)) {
              var count = map.get(bucket);
              return String.valueOf(count);
            } else {
              return "0";
            }
          };
      var queueTrace =
          plotly.writeTraceHtml(
              traceNameId,
              "by queue " + queueName,
              new Dates.BucketIterator(start, end, this.window),
              gen);
      traceIds.add(traceNameId);
      traces.add(queueTrace);
    }

    Function<Long, String> genSchema =
        (bucket) -> {
          if (schemaOpsCounts.containsKey(bucket)) {
            var count = schemaOpsCounts.get(bucket);
            return String.valueOf(count);
          } else {
            return "0";
          }
        };
    traceIds.add("schemaQueries");
    traces.add(
        plotly.writeTraceHtml(
            "schemaQueries",
            "refresh, drop, alter, create queries",
            new Dates.BucketIterator(start, end, this.window),
            genSchema));

    return plotly.writePlotHtml(
        "Queries.json queries active per %s"
            .formatted(Human.getHumanDurationFromMillis(this.window)),
        "total_counts",
        traceIds.toArray(new String[0]),
        traces.toArray(new String[0]));
  }
}
