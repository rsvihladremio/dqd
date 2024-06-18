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

import com.dremio.support.diagnostics.shared.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/** entry point for the queries-json command */
public class Exec {
  private static final Logger logger = Logger.getLogger(Exec.class.getName());

  /**
   * starts the queries-json command
   *
   * @param files filepath, directory, or comma separated list of files to read
   */
  public void run(
      final Collection<PathAndStream> files,
      final Function<Stream<Query>, Report> getReport,
      final Reporter reporter)
      throws IOException {
    // get total cpu count for thread pools
    final int processors = Runtime.getRuntime().availableProcessors();
    // set up a lock, so we can have results from parsing be safely handled
    ReentrantLock lock = new ReentrantLock();
    List<Stream<Query>> streams = new ArrayList<>();
    List<BufferedReader> readersToClose = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();
    // leave one core free
    final ExecutorService executor = Executors.newFixedThreadPool(processors - 1);
    try {
      logger.finer(() -> String.format("iteration through %d files", files.size()));
      for (PathAndStream path : files) {
        futures.add(
            executor.submit(
                () -> {
                  BufferedReader reader = null;
                  try {
                    logger.info("reading " + path.filePath());

                    reader =
                        new BufferedReader(
                            new InputStreamReader(path.stream(), StandardCharsets.UTF_8));
                    // simple jackson object mapper
                    ObjectMapper objectMapper = new ObjectMapper();
                    Stream<Query> queries =
                        reader
                            .lines()
                            .map(
                                x -> {
                                  try {
                                    return objectMapper.readValue(x, Query.class);
                                  } catch (JsonProcessingException e) {
                                    throw new RuntimeException(
                                        String.format("unable to parse file %s", path.filePath()),
                                        e);
                                  }
                                });
                    // lock so we can safely add the result to the streams list
                    lock.lock();
                    try {
                      // add streams
                      streams.add(queries);
                    } finally {
                      // release lock so other threads can get to work
                      lock.unlock();
                    }
                  } finally {
                    // if reader is not available don't try and close it
                    if (reader != null) {
                      // add it to close list
                      // we do this because streams are lazily evaluated
                      // if we go away from streams we will not need this
                      readersToClose.add(reader);
                    }
                  }
                  // parse
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
      // flat map the list of streams into a single stream
      // also remove all queries that are empty
      final Query empty = new Query();
      Stream<Query> mergedStreams =
          streams.stream().flatMap(x -> x).filter(Objects::nonNull).filter(x -> !x.equals(empty));
      logger.info("console reporter selected");
      // reporter = new ConsoleReporter();
      // report = new QueriesJsonTextReport(mergedStreams);
      Report report = getReport.apply(mergedStreams);
      reporter.output(report);
    } finally {
      // now that we have read the reports it is safe to close the readers as the streams should
      // all
      // have been
      // resolved and the files _actually_ parsed
      for (BufferedReader r : readersToClose) {
        try {
          r.close();
        } catch (IOException e) {
          // if they can't close, this should just be a warning, there is no reason to stop trying
          // to close the rest of the files.
          logger.log(Level.WARNING, "error trying to close reader ", e);
        }
        // no longer need the thread pool go ahead and shut it down
        executor.shutdown();
      }
    }
  }
}
