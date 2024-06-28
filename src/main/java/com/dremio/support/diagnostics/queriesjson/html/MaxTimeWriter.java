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

import com.dremio.support.diagnostics.shared.Human;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MaxTimeWriter {
  private final PlotlyWriter plotly = new PlotlyWriter();
  private final long window;

  public MaxTimeWriter(final long window) {
    this.window = window;
  }

  private Function<Long, String> genFunction(Map<Long, Long> map) {
    final Function<Long, String> gen =
        (bucket) -> {
          if (map.containsKey(bucket)) {
            final Long usage = map.get(bucket);
            // round down to a second
            return Long.toString(usage / 1000);
          }
          return "0";
        };
    return gen;
  }

  public String generate(
      final long startEpochMs,
      final long finishEpochMs,
      final Map<Long, Long> pending,
      final Map<Long, Long> metadataretrieval,
      final Map<Long, Long> queued,
      final Map<Long, Long> planning,
      final Map<Long, Long> pool) {
    // make a quick supplier that gives us a new date iterator every time we ask for it with the
    // same start, finish and window
    Supplier<Dates.BucketIterator> genDates =
        () -> new Dates.BucketIterator(startEpochMs, finishEpochMs, this.window);
    var pendingTrace =
        plotly.writeTraceHtml(
            "maxPending", "max seconds pending time", genDates.get(), genFunction(pending));
    var metadataTrace =
        plotly.writeTraceHtml(
            "maxMetadata",
            "max seconds metadata retrieval",
            genDates.get(),
            genFunction(metadataretrieval));
    ;
    var queuedTrace =
        plotly.writeTraceHtml(
            "maxQueued", "max seconds queued", genDates.get(), genFunction(queued));
    var planningTrace =
        plotly.writeTraceHtml(
            "maxPlanning", "max seconds in planning", genDates.get(), genFunction(planning));
    var poolTrace =
        plotly.writeTraceHtml(
            "maxPool", "max seconds in pool waiting", genDates.get(), genFunction(pool));

    return plotly.writePlotHtml(
        "Queries.json max values per %s".formatted(Human.getHumanDurationFromMillis(window)),
        "max_values",
        new String[] {"maxPending", "maxMetadata", "maxQueued", "maxPlanning", "maxPool"},
        pendingTrace,
        metadataTrace,
        queuedTrace,
        planningTrace,
        poolTrace);
  }
}
