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

import com.dremio.support.diagnostics.repro.ArgSetup;
import com.dremio.support.diagnostics.repro.Exec;
import com.dremio.support.diagnostics.repro.Exec.ResponseMessage;
import com.dremio.support.diagnostics.repro.SqlOutput;
import com.dremio.support.diagnostics.repro.parse.ColumnDefYaml;
import com.dremio.support.diagnostics.repro.parse.ReproProfileParserImpl;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import picocli.CommandLine;
import picocli.CommandLine.Help.Visibility;

/**
 * Repro is the picolci command binding for generating a schema file and is designed to replace the
 *
 * @see <a href=
 *     "https://github.com/dremio/dremio/tree/master/extras/tools/reproduction-tool">reproduction
 *     tool</a>. If you are interested in how the reproduction tool/schema generation works this is
 *     a good entry point
 */
@CommandLine.Command(
    // sets the name of the subcommand to repro, so `dqd repro`
    name = "repro",
    // the help command description that is printed out into the cli
    description =
        "given a profile.json or zip containing one, this command will generate a file with schemas"
            + " for all of the pds and vds found in the profile. It will first attempt to use the"
            + " arrow schema and then failing that fall back to using a guess based on the query",
    footer =
        "\n"
            + "#### EXAMPLES\n"
            + "~~~~~~~~~~~~~\n\n"
            + "##### Run a reproduction against a server\n"
            + "      note the operations are idempotent\n\n"
            + "\tdqd repro --host http://localhost:9047 -u user -p pass profile.zip\n\n"
            + "##### override default column generation with default creds and host\n\n"
            + "\tdqd repro --column-def-yaml columnDef.yaml"
            + " profile.json\n\n"
            + "###### EXAMPLE Column Definition Yaml\n"
            + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
            + "tables:\n"
            + " - name: ns1.table1\n"
            + "   columns:\n"
            + "     - name: status\n"
            + "       values: ['Active', 'Inactive', 'Suspended' ]\n"
            + "     - name: customerType\n"
            + "       values: [ 'Staff', 'User', 'Admin' ]\n",
    subcommands = CommandLine.HelpCommand.class)
public class Repro implements Callable<Integer> {
  static class ValidCtasFormats extends ArrayList<String> {
    private static final long serialVersionUID = 202301261800L;

    ValidCtasFormats() {
      super(Arrays.asList("PARQUET", "ICEBERG"));
    }
  }

  private static final Logger logger = Logger.getLogger(Repro.class.getName());

  // the file parameter that comes in as the first non command argument (not flag)
  // of the
  // dqd command, in the following case:
  // dqd repro ./profile_attempt_0.json
  // profile_attempt_0.json will be mapped here
  @CommandLine.Parameters(
      index = "0",
      description = "file to analyze, can be the profile.json itself or a zip file containing one")
  private File file;

  @CommandLine.Option(
      names = {"--column-def-yaml"},
      description = "column definitions that override the default random file generation")
  private File columnDef;

  /** http url for the rest api */
  @CommandLine.Option(
      names = {"--host"},
      defaultValue = "http://localhost:9047",
      description =
          "the http url of the dremio server which is used to submit sql and create spaces")
  private String dremioHost;

  /** dremio user for the rest api */
  @CommandLine.Option(
      names = {"--user", "-u"},
      defaultValue = "dremio",
      description = "the user used to submit sql and create spaces to the rest api")
  private String dremioUser;

  /** dremio password for the api user */
  @CommandLine.Option(
      names = {"--password", "-p"},
      interactive = false,
      defaultValue = "dremio123",
      description = "the password of the user used to submit sql and create spaces to the rest api")
  private String dremioPassword;

  /** default ctas format to use for data sources */
  @CommandLine.Option(
      names = {"--default-ctas-format"},
      completionCandidates = ValidCtasFormats.class,
      description = "set the default ctas format to use for datasources: ${COMPLETION-CANDIDATES}")
  private String defaultCtasFormat;

  // the number of records to output into PDSs that are generated
  @CommandLine.Option(
      names = {"-n", "--number-records"},
      description = "the number of rows to generate when creating PDSs",
      defaultValue = "20",
      showDefaultValue = Visibility.ALWAYS)
  private long records;

  /** timeout to use for PDS and VDS creation */
  @CommandLine.Option(
      names = {"-t", "--timeout-seconds"},
      description = "default timeout in seconds PDS and VDS creation",
      defaultValue = "60",
      showDefaultValue = Visibility.ALWAYS)
  private int timeoutSeconds;

  /** base dir to use for NAS source creation */
  @CommandLine.Option(
      names = {"--nas-source-base-dir"},
      description =
          "base dir to use for NAS source creation, this must be accessible to all executors",
      defaultValue = "")
  private String nasSourceBaseDir;

  @CommandLine.Option(
      names = {"--skip-ssl-verification"},
      description = "whether to skip ssl verification for queries or not",
      defaultValue = "false")
  private boolean skipSSLVerification;

  /**
   * call() handles exit code generation and is the catch all for error handling. This also is where
   * the command line flags are actually parsed to the Exec#run instance method.
   *
   * @return exit code passed to the CLI. 0 is success, anything else is failure.
   */
  @Override
  public Integer call() {
    try (FileInputStream fs = new FileInputStream(this.file)) {
      ColumnDefYaml columnDefYaml;
      if (columnDef != null) {
        final Yaml yaml = new Yaml(new Constructor(ColumnDefYaml.class, new LoaderOptions()));
        try (final InputStream st = Files.newInputStream(columnDef.toPath())) {
          columnDefYaml = yaml.load(st);
        }
      } else {
        columnDefYaml = new ColumnDefYaml();
        columnDefYaml.setTables(new ArrayList<>());
      }
      final ReproProfileParserImpl profileParser =
          ArgSetup.getReproProfile(this.records, columnDefYaml);
      final ProfileProvider profileProvider =
          ArgSetup.getProfileProvider(new PathAndStream(this.file.toPath(), fs));
      final SqlOutput[] sqlOutput =
          ArgSetup.getSqlOutput(
              dremioUser,
              dremioPassword,
              dremioHost,
              null,
              null,
              null,
              timeoutSeconds,
              nasSourceBaseDir,
              skipSSLVerification);
      final Exec exec =
          new Exec(
              Optional.ofNullable(defaultCtasFormat), profileProvider, profileParser, sqlOutput);
      ResponseMessage responseMessage = exec.run();
      return responseMessage.getErrorCode();
    } catch (final Exception e) {
      // log out the error to the console and then return an exit code
      logger.severe(
          () -> String.format("unable to run repro due to unhandled error %s", e.getMessage()));
      logger.log(Level.FINE, e, () -> "full exception");
      return 1;
    }
  }
}
