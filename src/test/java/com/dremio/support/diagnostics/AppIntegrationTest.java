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

import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemErrAndOut;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class AppIntegrationTest {

  static final String queriesCmd = "profile-json";

  @Nested
  class VerboseLogTest {

    @Test
    void testWarningDefault() throws Exception {
      final Logger logger = Logger.getLogger(VerboseLogTest.class.getName());
      App app = new App();
      app.setLogging(logger);
      assertThat(logger.getLevel()).isEqualTo(Level.WARNING);
    }

    @Test
    void testInfoAtVerbose() throws Exception {
      final Logger logger = Logger.getLogger(VerboseLogTest.class.getName());
      App app = new App();
      app.verbose = new boolean[] {true};
      app.setLogging(logger);
      assertThat(logger.getLevel()).isEqualTo(Level.INFO);
    }

    @Test
    void testDebugAtDoubleVerbose() throws Exception {
      final Logger logger = Logger.getLogger(VerboseLogTest.class.getName());
      App app = new App();
      app.verbose = new boolean[] {true, true};
      app.setLogging(logger);
      assertThat(logger.getLevel()).isEqualTo(Level.FINE);
    }

    @Test
    void testTraceAtTripleVerbose() throws Exception {
      final Logger logger = Logger.getLogger(VerboseLogTest.class.getName());
      App app = new App();
      app.verbose = new boolean[] {true, true, true};
      app.setLogging(logger);
      assertThat(logger.getLevel()).isEqualTo(Level.FINER);
    }

    @Test
    void testDiagAtQuadrupleVerbose() throws Exception {
      final Logger logger = Logger.getLogger(VerboseLogTest.class.getName());
      App app = new App();
      app.verbose = new boolean[] {true, true, true, true};
      app.setLogging(logger);
      assertThat(logger.getLevel()).isEqualTo(Level.FINEST);
    }
  }

  @Test
  void testThatEmptyCommandShowsHelp() throws Exception {
    String text =
        tapSystemErrAndOut(
            () -> {
              CommandLine cmd = new CommandLine(new App());
              cmd.execute();
            });
    assertThat(text).contains("Missing required subcommand");
  }

  @Test
  void testInvalidSubcommandParameter() throws Exception {
    String text =
        tapSystemErrAndOut(
            () -> {
              CommandLine cmd = new CommandLine(new App());
              cmd.execute(queriesCmd, "--alkjlkjklfdafsd", "wrong");
            });
    assertThat(text).contains("Unknown option: '--alkjlkjklfdafsd'");
  }
}
