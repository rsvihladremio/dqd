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
package com.dremio.support.diagnostics;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import com.dremio.support.diagnostics.cmds.IOStat;
import com.dremio.support.diagnostics.cmds.ProfileJson;
import com.dremio.support.diagnostics.cmds.QueriesJson;
import com.dremio.support.diagnostics.cmds.Repro;
import com.dremio.support.diagnostics.cmds.Server;
import com.dremio.support.diagnostics.cmds.Top;
import com.dremio.support.diagnostics.simple.ProfileJSONSimplified;
import java.util.Locale;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.ScopeType;

@Command(
    name = "dqd",
    description = "various commands run diagnostics and help automate support",
    subcommands = {
      CommandLine.HelpCommand.class,
      ProfileJson.class,
      ProfileJSONSimplified.Cli.class,
      QueriesJson.class,
      Repro.class,
      Server.class,
      Top.class,
      IOStat.class
    })
public class App {
  private static final int maxVerbosity = 3;
  private static final int traceVerbosity = 2;
  private static final int debubVerbosity = 1;

  public static void main(final String[] args) {
    Locale.setDefault(Locale.US);
    final App app = new App();
    final String rawVersion = app.getVersion();
    final String version;
    if (rawVersion == null) {
      version = "DEV";
    } else {
      version = rawVersion;
    }
    System.out.println("dqd version " + version); // NOPMD
    final int rc = new CommandLine(app).setExecutionStrategy(app::executionStrategy).execute(args);
    System.exit(rc);
  }

  @CommandLine.Option(
      scope = ScopeType.INHERIT,
      names = {"-v", "--verbose"},
      description = "-v for info, -vv for debug, -vvv for trace")
  boolean[] verbose;

  void setLogging(final Logger root) {
    final Level targetLevel = getTargetLevel();
    root.setLevel(targetLevel);
    for (final Handler handler : root.getHandlers()) {
      root.removeHandler(handler);
    }
    final CustomLogFormatter logFormatter = new CustomLogFormatter();
    final StreamHandler sh = new StreamHandler(System.out, logFormatter);
    sh.setLevel(targetLevel);
    root.addHandler(sh);
    if (FINEST.equals(targetLevel)) {
      root.info("MAX logging enabled");
    } else if (FINER.equals(targetLevel)) {
      root.info("TRACE logging enabled");
    } else if (FINE.equals(targetLevel)) {
      root.info("DEBUG logging enabled");
    } else if (INFO.equals(targetLevel)) {
      root.info("INFO logging enabled");
    } else if (WARNING.equals(targetLevel)) {
      root.info("WARN logging enabled");
    } else if (SEVERE.equals(targetLevel)) {
      root.info("ERROR logging enabled");
    }
  }

  // A reference to this method can be used as a custom execution strategy
  // this sets java.util.logging logging level for the default logger
  int executionStrategy(final ParseResult parseResult) {
    final Logger root = Logger.getLogger("");
    setLogging(root);
    return new CommandLine.RunLast().execute(parseResult); // default execution strategy
  }

  private Package getPackage() {
    return this.getClass().getPackage();
  }

  private String getVersion() {
    return this.getPackage().getImplementationVersion();
  }

  private Level getTargetLevel() {
    final int verboseVs;
    if (verbose != null) {
      verboseVs = verbose.length;
    } else {
      return WARNING;
    }
    if (verboseVs > maxVerbosity) {
      return FINEST;
    } else if (verboseVs > traceVerbosity) {
      return FINER;
    } else if (verboseVs > debubVerbosity) {
      return FINE;
    }
    return INFO;
  }
}
