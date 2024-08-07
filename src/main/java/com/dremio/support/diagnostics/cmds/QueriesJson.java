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
package com.dremio.support.diagnostics.cmds;

import com.dremio.support.diagnostics.queriesjson.Exec;
import com.dremio.support.diagnostics.queriesjson.QueriesJsonFileParser;
import com.dremio.support.diagnostics.queriesjson.QueriesJsonHtmlReport;
import com.dremio.support.diagnostics.queriesjson.ReadArchive;
import com.dremio.support.diagnostics.queriesjson.SearchedFile;
import com.dremio.support.diagnostics.queriesjson.filters.DateRangeQueryFilter;
import com.dremio.support.diagnostics.queriesjson.reporters.*;
import com.dremio.support.diagnostics.shared.Reporter;
import com.dremio.support.diagnostics.shared.StreamWriterReporter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import picocli.CommandLine;

/**
 * QueriesJSON is the picocli command binding for generating a if you are
 * interested in how `dqd
 * queries-json` works this is a good entry point.
 */
@CommandLine.Command(
    name = "queries-json",
    description = "analyze a queries.json file and make recommendations",
    footer =
        "\n"
            + "#### EXAMPLES\n"
            + "~~~~~~~~~~~~~\n\n"
            + "##### Generate summary analysis of one or several queries.json in the CLI:\n\n"
            + "\tdqd queries-json ./queries.json output.html\n\n"
            + "\tdqd queries-json ./queries.zip output.html\n\n"
            + "\tdqd queries-json ./queriesjsons/ output.html\n\n",
    subcommands = CommandLine.HelpCommand.class)
public class QueriesJson implements Callable<Integer> {

  @CommandLine.Option(
      names = {"-w", "--window-size"},
      defaultValue = "86400000",
      description = "window size in milliseconds to use for reporting",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Long window;

  // the file parameter that comes in as the first non-command argument (not flag)
  // of the
  // dqd command, in the following case:
  // dqd queries-json ./queriesjsonfiles ./queries.html
  // ./queriesjsonfiles will be mapped here
  @CommandLine.Parameters(
      index = "0",
      description =
          "file path to analyze, can be a directory where all *.json files will be analyzed."
              + "can be a list separated by commas, and finally can just be a single file")
  private File file;

  // the file parameter that comes in as the second non-command argument (not
  // flag) of the
  // dqd command, in the following case:
  // dqd queries-json ./queriesjsonfiles ./queries.html
  // ./queries.html will be mapped here
  @CommandLine.Parameters(
      index = "1",
      description =
          "file path to the report that we want to output, can be a directory where all *.json"
              + " files will be analyzed.can be a list separated by commas, and finally can just be"
              + " a single file")
  private File outputFile;

  @CommandLine.Option(
      names = {"-s", "--start"},
      defaultValue = "2000-01-01",
      description = "filter out all queries that start before this value",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private LocalDate start;

  @CommandLine.Option(
      names = {"-e", "--end"},
      defaultValue = "2070-01-01",
      description = "filter out all queries that start after this value",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private LocalDate end;

  @CommandLine.Option(
      names = {"--limit"},
      defaultValue = "1",
      description = "limit for problematic queries only works with INTERACTIVE report",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer limit;

  /**
   * call() takes the values from the command line flags and just passes those
   * values to the
   * Exec#run instance method.
   *
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Override
  public Integer call() throws IOException, InterruptedException, ExecutionException {
    try (var outputStream = Files.newOutputStream(outputFile.toPath())) {
      var startMs = start.toEpochSecond(LocalTime.of(0, 0, 0, 0), ZoneOffset.UTC) * 1000;
      var endMs = end.toEpochSecond(LocalTime.of(0, 0, 0, 0), ZoneOffset.UTC) * 1000;
      var filter = new DateRangeQueryFilter(startMs, endMs);

      final Reporter reporter = new StreamWriterReporter(outputStream);
      var reporters = new ArrayList<QueryReporter>();
      final ConcurrentQueriesReporter concurrentQueriesReporter =
          new ConcurrentQueriesReporter(this.window);
      reporters.add(concurrentQueriesReporter);
      final ConcurrentQueueReporter concurrentQueueReporter =
          new ConcurrentQueueReporter(this.window);
      reporters.add(concurrentQueueReporter);
      final ConcurrentSchemaOpsReporter concurrentSchemaOpsReporter =
          new ConcurrentSchemaOpsReporter(this.window);
      reporters.add(concurrentSchemaOpsReporter);
      final MaxMemoryQueriesReporter maxMemoryQueriesReporter =
          new MaxMemoryQueriesReporter(this.limit);
      reporters.add(maxMemoryQueriesReporter);
      final MaxCPUQueriesReporter maxCPUQueriesReporter = new MaxCPUQueriesReporter(this.limit);
      reporters.add(maxCPUQueriesReporter);
      final MaxTimeReporter maxTimeReporter = new MaxTimeReporter(this.window);
      reporters.add(maxTimeReporter);
      final MemoryAllocatedReporter memoryAllocatedReporter =
          new MemoryAllocatedReporter(this.window);
      reporters.add(memoryAllocatedReporter);
      final RequestCounterReporter requestCounterReporter = new RequestCounterReporter();
      reporters.add(requestCounterReporter);
      final RequestsByQueueReporter requestsByQueueReporter = new RequestsByQueueReporter();
      reporters.add(requestsByQueueReporter);
      final SlowestMetadataQueriesReporter slowestMetadataQueriesReporter =
          new SlowestMetadataQueriesReporter(this.limit);
      reporters.add(slowestMetadataQueriesReporter);
      final SlowestPlanningQueriesReporter slowestPlanningQueriesReporter =
          new SlowestPlanningQueriesReporter(this.limit);
      reporters.add(slowestPlanningQueriesReporter);
      final StartFinishReporter startFinishReporter = new StartFinishReporter();
      reporters.add(startFinishReporter);
      final TotalQueriesReporter totalQueriesReporter = new TotalQueriesReporter();
      reporters.add(totalQueriesReporter);
      final FailedQueriesReporter failedQueriesReporter = new FailedQueriesReporter(limit);
      reporters.add(failedQueriesReporter);

      var archive = new ReadArchive(filter);
      var cpus = Runtime.getRuntime().availableProcessors() / 2;
      List<SearchedFile> filesSearched = new ArrayList<SearchedFile>();
      if (file.toString().endsWith(".tgz") || file.toString().endsWith(".tar.gz")) {
        filesSearched = archive.readTarGz(file.toString(), reporters, cpus).stream().toList();
      } else if (file.toString().endsWith(".tar.xz")) {
        filesSearched = archive.readTarXz(file.toString(), reporters, cpus).stream().toList();
      } else if (file.toString().endsWith(".tar.bzip2")) {
        filesSearched = archive.readTarBzip2(file.toString(), reporters, cpus).stream().toList();
      } else if (file.toString().endsWith(".tar")) {
        filesSearched = archive.readTar(file.toString(), reporters, cpus).stream().toList();
      } else if (file.toString().endsWith(".zip")) {
        filesSearched = archive.readZip(file.toString(), reporters, cpus).stream().toList();
      } else if (file.toString().endsWith(".gz")) {
        var searchedFile = archive.parseGzip(file.toString(), file.toPath(), reporters);
        filesSearched.add(searchedFile);
      } else if (file.toString().endsWith(".bzip2")) {
        var searchedFile = archive.parseBzip2(file.toString(), reporters);
        filesSearched.add(searchedFile);
      } else if (file.toString().endsWith(".json")) {
        try (var is = Files.newInputStream(file.toPath())) {
          var searchedFile =
              QueriesJsonFileParser.parseFile(
                  file.toString(), is, reporters, new DateRangeQueryFilter(startMs, endMs));
          filesSearched.add(searchedFile);
        }
      } else {
        System.out.println(
            "unknown extension for file "
                + file.toString()
                + ": only supported extensions are .tar, .tar.gz, .tgz,"
                + " tar.xz, tar.bzip2, .bzip2, .gz, .zip and .json");
        return 1;
      }
      new Exec()
          .run(
              new QueriesJsonHtmlReport(
                  filesSearched,
                  Instant.ofEpochMilli(startMs),
                  Instant.ofEpochMilli(endMs),
                  this.window,
                  concurrentQueriesReporter,
                  concurrentQueueReporter,
                  concurrentSchemaOpsReporter,
                  maxMemoryQueriesReporter,
                  maxCPUQueriesReporter,
                  maxTimeReporter,
                  memoryAllocatedReporter,
                  requestCounterReporter,
                  requestsByQueueReporter,
                  slowestMetadataQueriesReporter,
                  slowestPlanningQueriesReporter,
                  startFinishReporter,
                  totalQueriesReporter,
                  failedQueriesReporter,
                  limit),
              reporter);
      return 0;
    }
  }
}
