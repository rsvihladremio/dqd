package com.dremio.support.diagnostics.queriesjson.reporters;

import com.dremio.support.diagnostics.queriesjson.Query;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SlowestMetadataQueriesReporter implements QueryReporter {
  private final long limit;
  private List<Query> queries = new ArrayList<>();

  public synchronized List<Query> getQueries() {
    return queries;
  }

  public SlowestMetadataQueriesReporter(final long limit) {
    this.limit = limit;
  }

  @Override
  public synchronized void parseRow(final Query q) {
    queries.add(q);
    queries =
        new ArrayList<>(
            queries.stream()
                .sorted(Comparator.comparingLong(Query::getNormalizedMetadataRetrieval).reversed())
                .limit(limit)
                .toList());
  }
}
