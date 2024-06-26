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
