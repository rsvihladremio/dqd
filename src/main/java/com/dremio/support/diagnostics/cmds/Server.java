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

import com.dremio.support.diagnostics.server.DQDWebServer;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "server",
    description =
        "run a web ui that is similar to the CLI but more rich with interactive graphs, starts on"
            + " port 8080",
    subcommands = CommandLine.HelpCommand.class)
public class Server implements Runnable {

  @CommandLine.Option(
      names = {"-c", "--complexity-limit"},
      defaultValue = "1000000000",
      description =
          "The complexity limit guards one from running out of heap or generating a graph that is"
              + " unreadable when generating reports. This is the number of queries times the"
              + " number of 1 second buckets and is a proxy for how expensive the calculation will"
              + " be. The default should be fine for most modern hardware but if you would like you"
              + " can override it here",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Long complexityLimit;

  @CommandLine.Option(
      names = {"-p", "--port"},
      defaultValue = "8080",
      description = "port to run the server on",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer port;

  @CommandLine.Option(
      names = {"-f", "--fallback-resolution-seconds"},
      defaultValue = "3600",
      description =
          "used with complexity limit to set the granularity of graphs from 1 second to this"
              + " number.",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Long fallbackResolutionSeconds;

  @Override
  public void run() {
    try {
      DQDWebServer.start(complexityLimit, port);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
