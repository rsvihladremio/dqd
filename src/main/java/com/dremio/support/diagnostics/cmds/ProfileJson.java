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

import com.dremio.support.diagnostics.profilejson.Exec;
import com.dremio.support.diagnostics.profilejson.ProfileDifferenceReport;
import com.dremio.support.diagnostics.profilejson.singlefile.SingleFileExec;
import com.dremio.support.diagnostics.repro.ArgSetup;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * ProfileJson contains the command line bindings and is the primary entry point of the profile-json
 * subcommand
 */
@Command(
    name = "profile-json",
    description = "analyze a profile json and make recommendations",
    footer =
        """

#### EXAMPLES
~~~~~~~~~~~~~

##### Generate a summary analysis

\tdqd profile-json profile.zip

##### Compare two profiles and their plans

\tdqd profile-json 1st.zip -c 2nd.zip --show-plan-details

""",
    subcommands = CommandLine.HelpCommand.class)
public class ProfileJson implements Callable<Integer> {

  /**
   * the primary file to analyze, if there is no fileToCompare the analysis of this file will be
   * more intensive and look for problems
   */
  @CommandLine.Parameters(index = "0", description = "The file to analyze")
  private File file;

  /**
   * optional but when provided this will compare the file against itself in a report that looks for
   * differences between two profiles without providing any further analysis
   */
  @Option(
      names = {"-c", "--comparison-file"},
      description =
          "A file to compare against, when a second file is added this will provide an analysis"
              + " against the changes")
  private File fileToCompare;

  @Option(
      names = {"-d", "--show-plan-details"},
      description =
          "shows more detail for plan analysis. Works when analyzing a single profile or two"
              + " profiles")
  private boolean showPlanDetails;

  /**
   * the actual initialization of the profile-json subcommand, this is executed by pico-cli
   *
   * @return the exit code, zero is success anything else is an error
   * @throws Exception when the command fails
   */
  @Override
  public Integer call() throws Exception {

    try (FileInputStream fs = new FileInputStream(this.file)) {
      ProfileProvider profileProvider =
          ArgSetup.getProfileProvider(new PathAndStream(this.file.toPath(), fs));
      if (fileToCompare != null) {
        try (FileInputStream fsToCompare = new FileInputStream(this.fileToCompare)) {
          ProfileProvider profileToCompareProvider =
              ArgSetup.getProfileProvider(
                  new PathAndStream(this.fileToCompare.toPath(), fsToCompare));
          Exec exec =
              new Exec(new ProfileDifferenceReport(), profileProvider, profileToCompareProvider);
          exec.run();
        }
      } else {
        // only does console
        final SingleFileExec singleFileExec = new SingleFileExec(profileProvider);
        singleFileExec.run();
      }
    }
    return 0;
  }
}
