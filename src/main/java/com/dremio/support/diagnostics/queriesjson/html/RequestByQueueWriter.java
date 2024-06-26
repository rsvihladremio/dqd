package com.dremio.support.diagnostics.queriesjson.html;

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;

import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class RequestByQueueWriter {
  /**
   * generates a html fragment that shows the summary of query counts by queue name
   *
   * @param totalQueries total number of queries in report
   * @param requestsByQueue breakdown of counts by queue name
   */
  public static String generate(final long totalQueries, final Map<String, Long> requestsByQueue) {
    if (totalQueries == 0) {
      return "<h2>Total Queries by Queue</h2><p>No Queries Found</p>";
    }
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    return builder.generateTable(
        "totalQueriesByQueue",
        "Total Queries by Queue",
        Arrays.asList("Queue", "count", "%"),
        requestsByQueue.entrySet().stream()
            // sort by count (hightest count should be first in the list)
            .sorted((l, r) -> r.getValue().compareTo(l.getValue()))
            .map(
                x ->
                    // generate table rows
                    Arrays.<HtmlTableDataColumn<String, Number>>asList(
                        // queue name
                        col(x.getKey()),
                        // queue count with a grouping comma
                        col(String.format("%,d", x.getValue()), x.getValue()),
                        // % of total queries this queue represents
                        col(
                            String.format("%.2f", 100.0 * x.getValue() / totalQueries),
                            x.getValue())))
            .collect(Collectors.toList()));
  }
}
