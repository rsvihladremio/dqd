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
import com.dremio.support.diagnostics.shared.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/** entry point for the queries-json command */
public class Exec {

  /** standard logging field */
  private static final Logger logger = Logger.getLogger(Exec.class.getName());

  /** Jackson object mapper */
  private static final ObjectMapper objectMapper = new ObjectMapper();

  void parseFile(
      PathAndStream path,
      final Collection<QueryFilter> filters,
      final Collection<QueryReporter> reporters) throws IOException {
    logger.info("reading " + path.filePath());
    try (var reader =
        new BufferedReader(new InputStreamReader(path.stream(), StandardCharsets.UTF_8))) {
      reader
          .lines()
          .map(
              x -> {
                try {
                  return objectMapper.readValue(x, Query.class);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(
                      String.format("unable to parse file %s", path.filePath()), e);
                }
              })
          .filter(
              x -> {
                for (QueryFilter filter : filters) {
                  boolean valid = filter.isValid(x);
                  if (!valid) {
                    return false;
                  }
                }
                return true;
              })
          .forEach(
              x -> {
                for (var r : reporters) {
                  r.parseRow(x);
                }
              });
    }
  }

  /**
   * starts the queries-json command (has the following properties) - is multi-threaded -
   *
   * @param files filepath, directory, or comma separated list of files to read
   */
  public void run(
      final Collection<PathAndStream> files,
      final Collection<QueryFilter> filters,
      final Collection<QueryReporter> reporters,
      final Function<Stream<Query>, Report> getReport,
      final Reporter reporter)
      throws IOException {
    // get total cpu count for thread pools
    final int processors = Runtime.getRuntime().availableProcessors();
    var futures = new ArrayList<Future<?>>();
    // leave one core free
    final ExecutorService executor = Executors.newFixedThreadPool(processors - 1);
    logger.finer(() -> String.format("iteration through %d files", files.size()));
    for (PathAndStream path : files) {
      futures.add(executor.submit(() -> {
        try {
          parseFile(path, filters, reporters);
        } catch (IOException e) {
          logger.log(Level.SEVERE, e, ()->String.format("iteration through %d files", files.size()));
        }
      }));
    }
    // iterate through the futures, so we can block on the results
    for (Future<?> future : futures) {
      try {
        // we don't actually care about the results so just throw them away
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    //   Report report = getReport.apply(mergedStreams);
    // reporter.output(report);
    executor.shutdown();
  }
}
