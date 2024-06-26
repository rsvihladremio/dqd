package com.dremio.support.diagnostics.queriesjson.html;

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;

import com.dremio.support.diagnostics.queriesjson.Query;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class SlowestPlanningWriter {

  public static String generate(final long totalQueries, final Collection<Query> slowestPlanning) {
    final StringBuilder builder = new StringBuilder();
    if (totalQueries == 0) {
      builder.append("<h2>Slowest Planning</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    var htmlBuilder = new HtmlTableBuilder();
    Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    slowestPlanning.forEach(
        x ->
            rows.add(
                Arrays.asList(
                    col(x.getQueryId()),
                    col(Dates.format(Instant.ofEpochMilli(x.getStart())), x.getStart()),
                    col(
                        Human.getHumanDurationFromMillis(x.getFinish() - x.getStart()),
                        x.getFinish() - x.getStart()),
                    col(Human.getHumanDurationFromMillis(x.getPlanningTime()), x.getPlanningTime()),
                    col(x.getQueryText()))));
    builder.append(
        htmlBuilder.generateTable(
            "slowestPlaningQueriesTable",
            "Slowest Planning",
            Arrays.asList("query id", "start", "query duration", "planning time", "query"),
            rows));
    return builder.toString();
  }
}
