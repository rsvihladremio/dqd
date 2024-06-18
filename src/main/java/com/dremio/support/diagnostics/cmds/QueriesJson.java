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
import com.dremio.support.diagnostics.queriesjson.QueriesJsonHtmlReport;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.PathParser;
import com.dremio.support.diagnostics.shared.Reporter;
import com.dremio.support.diagnostics.shared.StreamWriterReporter;
import com.dremio.support.diagnostics.shared.zip.ArchiveDetection;
import com.dremio.support.diagnostics.shared.zip.Extraction;
import com.dremio.support.diagnostics.shared.zip.UnzipperImpl;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
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

  private static final Logger logger = Logger.getLogger(QueriesJson.class.getName());

  @CommandLine.Option(
      names = {"-c", "--complexity-limit"},
      defaultValue = "10000",
      description =
          "The complexity limit guards one from running out of heap or generating a graph that is"
              + " unreadable when generating reports. This is the number of queries times the"
              + " number of 1 second buckets and is a proxy for how expensive the calculation will"
              + " be. The default should be fine for most modern hardware but if you would like you"
              + " can override it here. When this is exceeded granularity will be reduced to the"
              + " number of seconds specified in --fallback-resolution-seconds or -f",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Long complexityLimit;

  @CommandLine.Option(
      names = {"-f", "--fallback-resolution-seconds"},
      defaultValue = "3600",
      description =
          "used with complexity limit to set the granularity of graphs from 1 second to this"
              + " number.",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Long fallbackResolutionSeconds;

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
  private Instant start;

  @CommandLine.Option(
      names = {"-e", "--end"},
      defaultValue = "2070-01-01",
      description = "filter out all queries that start after this value",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Instant end;

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
   */
  @Override
  public Integer call() {
    final UnzipperImpl unzipper = new UnzipperImpl();
    List<Extraction> extractions = new ArrayList<>();
    try {
      List<PathAndStream> streams = new ArrayList<>();
      for (String path : new PathParser().convertPathToFiles(file.getPath())) {
        Path filePath = Paths.get(path);
        final PathAndStream pathAndStream =
            new PathAndStream(filePath, Files.newInputStream(filePath));
        if (ArchiveDetection.isArchive(filePath.getFileName().toString())) {
          var result =
              unzipper.unzipAllFiles(
                  pathAndStream, x -> x.startsWith("queries") && x.contains(".json"));
          extractions.addAll(result);
          for (Extraction extraction : result) {
            streams.addAll(extraction.getPathAndStreams());
          }

        } else {
          streams.add(pathAndStream);
        }
      }
      try (var outputStream = Files.newOutputStream(outputFile.toPath())) {
        final Reporter reporter = new StreamWriterReporter(outputStream);

        new Exec()
            .run(
                streams.stream().toList(),
                x ->
                    new QueriesJsonHtmlReport(
                        limit, x, complexityLimit, fallbackResolutionSeconds, start, end),
                reporter);
      }
    } catch (Exception e) {
      // just log error and return a generic exit code of 1
      logger.log(Level.SEVERE, "unable to run executable", e);
      return 1;
    } finally {
      for (Extraction extraction : extractions) {
        try {
          extraction.close();
        } catch (IOException e) {
          logger.warning("unable to close zip file: %s".formatted(e));
        }
      }
    }
    return 0;
  }
}
