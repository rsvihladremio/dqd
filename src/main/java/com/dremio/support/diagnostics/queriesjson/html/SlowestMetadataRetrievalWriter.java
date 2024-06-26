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

public class SlowestMetadataRetrievalWriter {

  public static String generate(final long totalQueries, final Collection<Query> slowestMetadata) {
    final StringBuilder builder = new StringBuilder();
    if (totalQueries == 0) {
      builder.append("<h2>Slowest Metadata Retrieval</h2>");
      builder.append("<p>No Queries Found</p>");
      return builder.toString();
    }
    final var htmlBuilder = new HtmlTableBuilder();
    final Collection<Collection<HtmlTableDataColumn<String, Number>>> rows = new ArrayList<>();
    slowestMetadata.forEach(
        x ->
            rows.add(
                Arrays.asList(
                    // query id
                    col(x.getQueryId()),
                    // query start time as a well formatted object
                    col(Dates.format(Instant.ofEpochMilli(x.getStart())), x.getStart()),
                    // query duration
                    col(
                        Human.getHumanDurationFromMillis(x.getFinish() - x.getStart()),
                        x.getFinish() - x.getStart()),
                    // query metadata retrieval time
                    col(
                        Human.getHumanDurationFromMillis(x.getNormalizedMetadataRetrieval()),
                        x.getNormalizedMetadataRetrieval()),
                    // query text for the listed query
                    col(x.getQueryText()))));
    builder.append(
        htmlBuilder.generateTable(
            "metatdataRetrievalQueriesTable",
            "Slowest Metadata Retrieval",
            Arrays.asList(
                "query id", "start", "query duration", "metadata retrieval time", "query"),
            rows));
    return builder.toString();
  }
}
