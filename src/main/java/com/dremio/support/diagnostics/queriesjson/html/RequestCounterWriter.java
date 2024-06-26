package com.dremio.support.diagnostics.queriesjson.html;

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;

import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/** RequestCounterWriter generates an html fragment from request outcome data */
public class RequestCounterWriter {

  /**
   * generates a html fragment report for the outcomes and counts
   *
   * @param totalQueries total number of queries
   * @param requestCounterMap has a break down of request outcomes and their counts
   * @return an html table
   */
  public static String generate(
      final long totalQueries, final Map<String, Long> requestCounterMap) {
    final StringBuilder builder = new StringBuilder();
    if (totalQueries == 0) {
      builder.append("<h2>Query Outcomes</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    final var tableBuilder = new HtmlTableBuilder();
    final Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    requestCounterMap.entrySet().stream()
        // represents a row in the html table
        .map(x -> Arrays.asList(x.getKey(), x.getValue(), (100.0 * x.getValue()) / totalQueries))
        .forEach(
            x -> {
              // we could just map the values here instead of doing foreach but this was easier for
              // inferring the types when this was written, it may be easier to do in later versions
              // of Java
              rows.add(
                  Arrays.asList(
                      // outcome title
                      col(String.valueOf(x.get(0))),
                      // number of queries in that outcome
                      col(String.format("%,d", (Long) x.get(1)), (Long) x.get(1)),
                      // % of total queries this outcome represents
                      col(String.format("%.2f", ((Double) x.get(2))), (Double) x.get(2))));
            });
    // generate table with rows and titles
    builder.append(
        tableBuilder.generateTable(
            "queryOutcomes", "Query Outcomes", Arrays.asList("outcome", "count", "%"), rows));
    return builder.toString();
  }
}
