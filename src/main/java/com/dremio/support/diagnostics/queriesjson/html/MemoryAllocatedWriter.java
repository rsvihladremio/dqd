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

public class MemoryAllocatedWriter {

  private final PlotlyWriter plotly = new PlotlyWriter();
  private final long bytesInMb = 1048576L;
  private final long bucketSize;

  public MemoryAllocatedWriter(final long bucketSize) {
    this.bucketSize = bucketSize;
  }

  public String generate(
      final long startEpochMs,
      final long finishEpochMs,
      final Map<Long, Double> memoryUsageBuckets) {
    final String memoryAllocatedId = "memoryAllocated";
    final var datesIter = new Dates.BucketIterator(startEpochMs, finishEpochMs, this.bucketSize);
    final Function<Long, String> gen =
        (bucket) -> {
          if (memoryUsageBuckets.containsKey(bucket)) {
            final Double usage = memoryUsageBuckets.get(bucket);
            final Double usageMb = usage / bytesInMb;
            final long rounded = Math.round(usageMb);
            return Long.toString(rounded);
          }
          return "0";
        };
    final String memoryAllocatedTrace =
        plotly.writeTraceHtml(memoryAllocatedId, "bytes allocated", datesIter, gen);
    final String memTimeTitle =
        "Queries.json ESTIMATED memory allocated per %s in MB (1048576 bytes)"
            .formatted(Human.getHumanDurationFromMillis(bucketSize));
    return plotly.writePlotHtml(
        memTimeTitle, "memory_allocated", new String[] {memoryAllocatedId}, memoryAllocatedTrace);
  }
}
