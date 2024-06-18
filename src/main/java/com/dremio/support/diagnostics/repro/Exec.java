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
package com.dremio.support.diagnostics.repro;

import static java.lang.String.format;

import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * primary entry point for reproduction tooling. Look here to understand the flow of the program.
 */
public class Exec {
  private static final Logger LOGGER = Logger.getLogger(Exec.class.getName());
  private final ProfileProvider profileProvider;
  private final SqlOutput[] sqlOutput;
  private final ReproProfileParser parser;
  private final Optional<String> defaultCtasFormat;

  public static class ResponseMessage {
    private int errorCode;
    private String errorText;

    public int getErrorCode() {
      return errorCode;
    }

    public String getErrorText() {
      return errorText;
    }

    public ResponseMessage(int errorCode, String errorText) {
      this.errorCode = errorCode;
      this.errorText = errorText;
    }
  }

  /** Exec is the class with the logic that starts the repro subcommand. */
  public Exec(
      final Optional<String> defaultCtasFormat,
      final ProfileProvider profileProvider,
      final ReproProfileParser parser,
      final SqlOutput... sqlOutput) {
    this.defaultCtasFormat = defaultCtasFormat;
    this.profileProvider = profileProvider;
    this.sqlOutput = sqlOutput.clone();
    this.parser = parser;
  }

  private VdsReference createVdsReference(
      final VdsSql vds,
      final Collection<VdsSql> vdsCollection,
      final Collection<PdsSql> pdsCollection) {
    final Locale locale = Locale.US;
    String lowerCaseName = vds.getTableName().toLowerCase(locale);
    List<String> references = new ArrayList<>();
    List<String> missingReferences = new ArrayList<>();
    for (final String ref : vds.getTableReferences()) {
      final String lowerCaseRefName = ref.toLowerCase(locale);
      boolean isVds = false;
      for (VdsSql v : vdsCollection) {
        String vName = v.getTableName().toLowerCase(locale);
        if (vName.equals(lowerCaseRefName)) {
          // need to set this so we avoid adding this to the missing list later
          isVds = true;
          references.add(lowerCaseRefName);
          // founce we can break now
          break;
        }
      }
      boolean isPds = false;
      if (!isVds) {
        for (PdsSql p : pdsCollection) {
          String pName = p.getTableName().toLowerCase(locale);
          if (pName.equals(lowerCaseRefName)) {
            // need to set this so we avoid adding this to the missing list later
            isPds = true;
            references.add(lowerCaseRefName);
            // founce we can break now
            break;
          }
        }
      }
      if (!isVds && !isPds) {
        missingReferences.add(lowerCaseRefName);
      }
    }
    return new VdsReference(lowerCaseName, missingReferences, references);
  }

  /**
   * run() is the entry point for the repro subcommand, and runs without any of the picocli code.
   * This is therefore the best place to look for the actual command logic.
   *
   * @return exit code for app, 0 is success
   */
  public ResponseMessage run() throws IOException {
    int exitCode = 0;
    String errorText = "";
    try {
      if (this.sqlOutput.length == 0) {
        // no point in going on to the output phase
        String text = "no output options specified. This is a critical error exiting";
        LOGGER.severe(text);
        return new ResponseMessage(1, text);
      }
      final ProfileJSON profileJSON = this.profileProvider.getProfile();
      final Collection<VdsSql> vdss = this.parser.parseVDSs(profileJSON);
      final Collection<PdsSql> pdss = this.parser.parsePDSs(profileJSON);
      if (pdss.isEmpty()) {
        // no point in going on to the output phase
        final String text =
            "no PDSs found in the profile. Repro will fail without those PDSs in place";
        LOGGER.severe(text);
        Stream<String> sqlList = vdss.stream().map(x -> "====START VDS=====\n" + x.getSql());
        return new ResponseMessage(
            2,
            text
                + "\nbut here are the generated VDSs below:\n\n"
                + String.join("\n\n====END VDS=====\n", sqlList.toArray(String[]::new)));
      }
      final Collection<String> sources = this.parser.parseSources(profileJSON);
      final Collection<Collection<String>> folders = this.parser.parseFolders(profileJSON);
      final Collection<String> spaces = this.parser.parseSpaces(profileJSON);
      final Collection<VdsReference> vdsReferenceInfo =
          vdss.stream().map(x -> createVdsReference(x, vdss, pdss)).collect(Collectors.toList());
      LOGGER.info("vds order will be");
      vdsReferenceInfo.forEach(
          x ->
              LOGGER.info(
                  () ->
                      String.format(
                          "- %s - refs: \n\t\t%s-\n\t\t - missing refs: %s",
                          x.getName(),
                          String.join("\n\t\t", x.getValidReferences()),
                          String.join("\n\t\t", x.getMissingReferences()))));
      for (final SqlOutput output : sqlOutput) {
        try {
          LOGGER.info(() -> String.format("starting output to %s", output.getName()));
          final JobResult sourceCreateResult = output.sourceOutput(sources, defaultCtasFormat);
          if (!sourceCreateResult.getSuccess()) {
            LOGGER.severe(sourceCreateResult::toString);
            errorText = "there are errors creating sources, exiting " + output.getName();
            LOGGER.severe(errorText);
            exitCode = 1;
            // just skip, so we can keep trying the other output strategies..
            // we will still return an error code of 1 in the end
            continue;
          }
          final JobResult spaceCreateResult = output.spaceOutput(spaces);
          if (!spaceCreateResult.getSuccess()) {
            LOGGER.severe(spaceCreateResult::toString);
            errorText = format("there are errors creating spaces, exiting %s", output.getName());
            LOGGER.severe(errorText);
            exitCode = 1;
            // just skip, so we can keep trying the other output strategies.
            // we will still return an error code of 1 in the end
            continue;
          }
          final JobResult folderCreateResult = output.folderOutput(folders);
          if (!folderCreateResult.getSuccess()) {
            LOGGER.severe(folderCreateResult::toString);
            errorText = format("there are errors creating folders, exiting %s", output.getName());
            LOGGER.severe(errorText);
            exitCode = 1;
            // just skip, so we can keep trying the other output strategies.
            // we will still return an error code of 1 in the end
            continue;
          }
          final JobResult pdsCreateResult = output.writePDSs(pdss);
          if (!pdsCreateResult.getSuccess()) {
            LOGGER.severe(pdsCreateResult::toString);
            LOGGER.severe(
                () -> format("there are errors creating PDSs, exiting %s", output.getName()));
            exitCode = 1;
            // just skip, so we can keep trying the other output strategies.
            // we will still return an error code of 1 in the end
            continue;
          }
          final JobResult vdsCreateResult = output.writeVDSs(vdss, vdsReferenceInfo);
          if (!vdsCreateResult.getSuccess()) {
            LOGGER.severe(vdsCreateResult::toString);
            errorText = format("there are errors creating VDSs, exiting %s", output.getName());
            LOGGER.severe(errorText);
            exitCode = 1;
            // just skip, so we can keep trying the other output strategies.
            // we will still return an error code of 1 in the end
          }
        } finally {
          if (output != null) {
            output.close();
          }
        }
      }
    } catch (final Exception e) {
      LOGGER.log(Level.SEVERE, "unhandled error", e);
      errorText = "unhandled error: " + e.getMessage();
      exitCode = 3;
    }
    return new ResponseMessage(exitCode, errorText);
  }
}
