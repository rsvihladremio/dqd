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
package com.dremio.support.diagnostics.queriesjson;

import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.Report;
import com.github.freva.asciitable.AsciiTable;
import com.google.common.base.Splitter;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class QueriesJsonTextReport implements Report {
  private static final Logger logger = Logger.getLogger(QueriesJsonTextReport.class.getName());
  private final Stream<Query> queries;

  public QueriesJsonTextReport(Stream<Query> queries) {
    this.queries = queries;
  }

  @Override
  public String getText() {
    SummaryAggregator agg = new SummaryAggregator(this.queries);
    if (agg.isEmpty()) {
      return "no queries found";
    } else {
      logger.info(() -> agg.size() + " queries parsed");
    }
    Summary summary = agg.generateSummary();

    StringBuilder builder = new StringBuilder();
    String[] header = new String[] {"title", "value", "timestamp", "query"};
    String[][] rows = new String[5][];
    String maxMemoryTitle = "max memory allocated";
    if (summary.getMaxMemoryQuery() != null) {
      rows[0] =
          getRow(
              summary.getMaxMemoryQuery(),
              maxMemoryTitle,
              Human.getHumanBytes1024(summary.getMaxMemoryQuery().getMemoryAllocated()));
    } else {
      rows[0] = getNullRow(maxMemoryTitle, agg.getQueryCount());
    }
    String maxMetadataTitle = "max metadata retrieval duration";
    if (summary.getMaxMetadataRetrievalQuery() != null) {
      rows[1] =
          getRow(
              summary.getMaxMetadataRetrievalQuery(),
              maxMetadataTitle,
              Human.getHumanDurationFromMillis(
                  summary.getMaxMetadataRetrievalQuery().getNormalizedMetadataRetrieval()));
    } else {
      rows[1] = getNullRow(maxMetadataTitle, agg.getQueryCount());
    }
    String maxAttemptsTitle = "max attempts";
    if (summary.getMaxAttemptsQuery() != null) {
      rows[2] =
          getRow(
              summary.getMaxAttemptsQuery(),
              maxAttemptsTitle,
              String.valueOf(summary.getMaxAttemptsQuery().getAttemptCount()));
    } else {
      rows[2] = getNullRow(maxAttemptsTitle, agg.getQueryCount());
    }
    String maxPendingTitle = "max pending duration";
    if (summary.getMaxPendingQuery() != null) {
      rows[3] =
          getRow(
              summary.getMaxPendingQuery(),
              maxPendingTitle,
              Human.getHumanDurationFromMillis(summary.getMaxPendingQuery().getPendingTime()));
    } else {
      rows[3] = getNullRow(maxPendingTitle, agg.getQueryCount());
    }
    String maxCommandPoolTitle = "max command pool duration";
    if (summary.getMaxCommandPoolQuery() != null) {
      rows[4] =
          getRow(
              summary.getMaxCommandPoolQuery(),
              maxCommandPoolTitle,
              Human.getHumanDurationFromMillis(summary.getMaxCommandPoolQuery().getPoolWaitTime()));
    } else {
      rows[4] = getNullRow(maxCommandPoolTitle, agg.getQueryCount());
    }
    builder.append(AsciiTable.getTable(header, rows));

    Bucket busiestBucket = summary.getBusiestBucket();
    builder.append("\nBusiest Bucket is at :");
    builder.append(Instant.ofEpochMilli(busiestBucket.getTimestamp()).toString());
    builder.append(" with ");
    builder.append(busiestBucket.getQueries().size());
    builder.append(" queries\n");
    header =
        new String[] {"query start", "query finish", "hh:mm:ss.SSS duration", "query (truncated)"};
    List<SummaryQuery> busiestBucketQueries = busiestBucket.getQueries();
    rows = new String[busiestBucketQueries.size()][];
    busiestBucketQueries.sort(Comparator.comparing(SummaryQuery::getStartEpochMillis));
    for (int i = 0; i < busiestBucketQueries.size(); i++) {
      SummaryQuery summaryQuery = busiestBucketQueries.get(i);
      rows[i] =
          new String[] {
            Instant.ofEpochMilli(summaryQuery.getStartEpochMillis()).toString(),
            Instant.ofEpochMilli(summaryQuery.getFinishEpochMillis()).toString(),
            Human.getHumanDurationFromMillis(
                summaryQuery.getFinishEpochMillis() - summaryQuery.getStartEpochMillis()),
            reformatSql(summaryQuery.getQueryText()),
          };
    }
    builder.append(AsciiTable.getTable(header, rows));
    return builder.toString();
  }

  private String[] getNullRow(String text, int queryCount) {
    return new String[] {
      text, "0", "N/A", String.format("there were %,d queries with a value of zero", queryCount)
    };
  }

  static String reformatSql(String sqlStr) {
    StringBuilder builder = new StringBuilder();
    for (String line : Splitter.on('\n').split(sqlStr)) {
      builder.append(line.replaceAll("\\s+", " "));
      builder.append("\n");
    }
    return builder.toString();
  }

  static String[] getRow(Query query, String text, String value) {
    return new String[] {
      text,
      value,
      Instant.ofEpochMilli(query.getStart()).toString(),
      reformatSql(query.getQueryText())
    };
  }

  @Override
  public String getTitle() {
    return "queries.json summary report";
  }
}
