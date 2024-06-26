package com.dremio.support.diagnostics.queriesjson.html;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.dremio.support.diagnostics.shared.Human;

public class MaxTimeWriter {
    private final PlotlyWriter plotly = new PlotlyWriter();
    private final long window;

    public MaxTimeWriter(final long window){
        this.window = window;
    }

    private Function<Long, String> genFunction(Map<Long, Long> map) {
        final Function<Long, String> gen =
        (bucket) -> {
          if (map.containsKey(bucket)) {
            final Long usage = map.get(bucket);
            //round down to a second
            return Long.toString(usage /1000);
          }
          return "0";
        };
        return gen;
    }

    public String generate(final long startEpochMs, final long finishEpochMs,
    final Map<Long, Long> pending, 
    final Map<Long, Long> attempts,
    final Map<Long, Long> queued,
    final Map<Long, Long> planning
    ){
        // make a quick supplier that gives us a new date iterator every time we ask for it with the same start, finish and window
        Supplier<Dates.BucketIterator> genDates = ()-> new Dates.BucketIterator(startEpochMs, finishEpochMs, this.window);
        var pendingTrace = plotly.writeTraceHtml("maxPending", "max seconds pending time", genDates.get(), genFunction(pending));
        var attemptsTrace = plotly.writeTraceHtml("maxAttempts", "max attempts", genDates.get(), genFunction(attempts));;
        var queuedTrace =  plotly.writeTraceHtml("maxQueued", "max seconds queued", genDates.get(), genFunction(queued));
        var planningTrace =  plotly.writeTraceHtml("maxPlanning", "max seconds in planning", genDates.get(), genFunction(planning));

        return plotly.writePlotHtml(
                "Queries.json max values per %s"
                    .formatted(Human.getHumanDurationFromMillis(window)),
                "max_values",
                new String[] {"maxPending", "maxAttempts", "maxQueued", "maxPlanning"},
                pendingTrace,
                attemptsTrace,
                queuedTrace,
                planningTrace);
    }
}
