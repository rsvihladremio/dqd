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

import com.dremio.support.diagnostics.queriesjson.filters.QueryFilter;
import com.dremio.support.diagnostics.queriesjson.reporters.QueryReporter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.logging.Logger;

public class QueriesJsonFileParser {
  private static final Logger LOGGER = Logger.getLogger(QueriesJsonFileParser.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * core logic for the file parse, use if the file is gzipped or if the file is a text file
   * @param fileName original archive entry name used for reporting purposes only
   * @param is input stream to parse
   * @param reports list of reporters to run against each query
   * @returns a searched file with the file name, number of records parsed and records filtered
   * @throws JsonMappingException comes from jackson when we're unable to map the string
   * @throws JsonProcessingException also comes from jackson
   * @throws IOException when we're unable to read the input stream
   */
  public static SearchedFile parseFile(
      String fileName, InputStream is, Collection<QueryReporter> reports, QueryFilter queryFilter)
      throws JsonMappingException, JsonProcessingException, IOException {
    LOGGER.info("parsing entry %s".formatted(fileName));
    try (BufferedReader r = new BufferedReader(new InputStreamReader(is))) {
      final Instant startTime = Instant.now();
      String line;
      // count is only for reporting how many queries were in each file
      long count = 0;
      long filtered = 0;
      // we read each line in the file and if the line is null we exit.
      while (null != (line = r.readLine())) {
        // standard jackson code to read an object from a string
        // at some point we should consider the stream api to see if we can get more parsing speed
        final Query query = mapper.readValue(line, Query.class);
        if (!queryFilter.isValid(query)) {
          filtered++;
          continue;
        }
        // we don't count filtered queries for the main count
        count++;
        for (QueryReporter reporter : reports) {
          reporter.parseRow(query);
        }
      }
      final long totalFiltered = filtered;
      final long totalCount = count;
      final Instant endTime = Instant.now();
      // we log the parse duration for performance changes between versions
      final Duration totalTime = Duration.between(startTime, endTime);
      LOGGER.info(
          () ->
              String.format(
                  "%d queries parsed (%d filtered by -s and -e flags) in %s millis from file %s",
                  totalCount, totalFiltered, totalTime.toMillis(), fileName));
      return new SearchedFile(totalFiltered, totalCount, fileName, "");
    }
  }
}
