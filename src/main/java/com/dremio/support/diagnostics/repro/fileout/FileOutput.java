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
package com.dremio.support.diagnostics.repro.fileout;

import static java.lang.String.*;

import com.dremio.support.diagnostics.repro.*;
import com.dremio.support.diagnostics.shared.FileMaker;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * outputs all output to a series of files, it also generates a script to actually create spaces and
 * post the sql
 */
public abstract class FileOutput implements SqlOutput {
  private final Path sqlDir;
  private final FileMaker fileMaker;
  private final StringWriter bufferedWriter;
  private final StringWriter debugWriter;
  private final StringWriter sqlWriter;

  /**
   * @param timeoutSeconds timeout to give for creation of pds and vds
   * @param fileMaker generator for source directories
   */
  protected FileOutput(final int timeoutSeconds, final FileMaker fileMaker) {
    this.fileMaker = fileMaker;
    bufferedWriter = new StringWriter();
    bufferedWriter.write(format("#!/bin/bash%n%n"));
    bufferedWriter.write(format("POSITIONAL_ARGS=()%n%n"));
    bufferedWriter.write(format("while [[ $# -gt 0 ]]; do%n"));
    bufferedWriter.write(format("\tcase $1 in%n"));
    bufferedWriter.write(format("\t--help)%n"));
    bufferedWriter.write(format("\t\tSHOW_HELP=\"true\"%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\t;;%n"));
    bufferedWriter.write(format("\t-u|--username)%n"));
    bufferedWriter.write(format("\t\tDREMIO_USER=$2%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\t;;%n"));
    bufferedWriter.write(format("\t-p|--password)%n"));
    bufferedWriter.write(format("\t\tDREMIO_PASS=$2%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\t;;%n"));
    bufferedWriter.write(format("\t-h|--host)%n"));
    bufferedWriter.write(format("\t\tDREMIO_HOST=$2%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\t;;%n"));
    bufferedWriter.write(format("\t--*|--*)%n"));
    bufferedWriter.write(format("\t\t\"Unknown option $1\"%n"));
    bufferedWriter.write(format("\t\texit 1%n"));
    bufferedWriter.write(format("\t\t;;%n"));
    bufferedWriter.write(format("\t--*|--*)%n"));
    bufferedWriter.write(format("\tPOSITIONAL_ARGS+=(\"$1\")%n"));
    bufferedWriter.write(format("\t\tshift%n"));
    bufferedWriter.write(format("\t\t;;%n"));
    bufferedWriter.write(format("\t\tesac%n"));
    bufferedWriter.write(format("\t\tdone%n"));
    bufferedWriter.write(format("\t\tset -- \"${POSITIONAL_ARGS[@]}\"%n"));
    bufferedWriter.write(format("%n"));
    bufferedWriter.write(format("%n"));
    bufferedWriter.write("if [ \"$SHOW_HELP\" = \"true\" ]; then\n");
    bufferedWriter.write("echo \"");
    bufferedWriter.write(getREADMEText());
    bufferedWriter.write("\"\n");
    bufferedWriter.write("exit 0\n");
    bufferedWriter.write("fi\n");
    bufferedWriter.write(
        "TOKEN=$(curl -X POST ${DREMIO_HOST}/apiv2/login -H 'Content-Type: application/json' -d"
            + " \"{\\\"userName\\\": \\\"$DREMIO_USER\\\", \\\"password\\\": \\\"$DREMIO_PASS\\\""
            + " }\"| grep token | awk -F ':\"' '{print $2}' | awk -F ',' '{print $1}' | tr -d"
            + " '\"')");
    bufferedWriter.write(format("%necho \"token is _dremio${TOKEN}\"%n"));
    bufferedWriter.write(
        "mkdiriflocal(){\n"
            + "\tif test \"${DREMIO_HOST#*localhost}\" != \"$DREMIO_HOST\"\n"
            + "\tthen\n"
            + "\t\tmkdir $1\n"
            + "\tfi\n"
            + "}\n");
    bufferedWriter.write("JOB_ID=\"\"\n");
    bufferedWriter.write("declare -i FAIL_COUNT=0\n");
    bufferedWriter.write("declare -i LAST_FAIL_COUNT=0\n");
    bufferedWriter.write("function check_job () {\n");
    bufferedWriter.write(format("\tfor i in {1..%s}\n", timeoutSeconds));
    bufferedWriter.write("\tdo\n");
    bufferedWriter.write("\t\techo \"checking status of job id $JOB_ID\"\n");
    bufferedWriter.write("\t\tsleep 1\n");
    bufferedWriter.write(
        "\t\tJOB_RESULT=$(curl -s -X GET "
            + "${DREMIO_HOST}/api/v3/job/${JOB_ID} "
            + "  -H \"Authorization: _dremio${TOKEN}\" "
            + "  -H 'Content-Type: application/json')\n");
    bufferedWriter.write("\t\tif [[ \"$JOB_RESULT\" == *\"already exists\"* ]]; then\n");
    bufferedWriter.write("\t\t\techo \"already exists skipping\"\n");
    bufferedWriter.write("\t\t\tbreak\n");
    bufferedWriter.write("\t\tfi\n");
    bufferedWriter.write("\t\tif [[ \"$JOB_RESULT\" == *\"FAILED\"* ]]; then\n");
    bufferedWriter.write("\t\t\techo \"failed job will try again\"\n");
    bufferedWriter.write("\t\t\tFAIL_COUNT+=1\n");
    bufferedWriter.write("\t\t\tbreak\n");
    bufferedWriter.write("\t\tfi\n");
    bufferedWriter.write("\t\tif [[ \"$JOB_RESULT\" == *\"COMPLETED\"* ]]; then\n");
    bufferedWriter.write("\t\t\techo \"${JOB_ID} completed\"\n");
    bufferedWriter.write("\t\t\tbreak\n");
    bufferedWriter.write("\t\telse\n");
    bufferedWriter.write("\t\t\techo \"attempt ${i} of " + timeoutSeconds + " for ${JOB_ID}\"\n");
    bufferedWriter.write("\t\tfi\n");
    bufferedWriter.write("\tdone\n");
    bufferedWriter.write("}\n");
    bufferedWriter.write("declare -i TIMES_NO_CHANGE=0\n");
    bufferedWriter.write("for ((i = 0 ; i < 100 ; i++)); do\n");
    bufferedWriter.write("\techo \"$FAIL_COUNT queries failed\" \n");
    bufferedWriter.write("\techo \"$LAST_FAIL_COUNT queries run prior\" \n");
    bufferedWriter.write(
        "\tif [ \\( \"$LAST_FAIL_COUNT\" = \"$FAIL_COUNT\" \\) -a \\( \"$i\" -gt \"0\" \\) ];"
            + " then\n");
    bufferedWriter.write("\t\tTIMES_NO_CHANGE+=1\n");
    bufferedWriter.write("\t\tif [ \"$TIMES_NO_CHANGE\" -gt \"2\" ]; then\n");
    bufferedWriter.write(
        "\t\t\t\techo \"exiting since the script cannot figure out what is wrong, this suggests a"
            + " critical error, read the debug.log or make a Support Tools Jira with the error and"
            + " the debug.log\"\n");
    bufferedWriter.write("\t\t\t\texit 1\n");
    bufferedWriter.write("\t\tfi\n");
    bufferedWriter.write("\telse TIMES_NO_CHANGE=0 \n");
    bufferedWriter.write("\tfi\n");
    bufferedWriter.write("LAST_FAIL_COUNT=$FAIL_COUNT\n");
    this.debugWriter = new StringWriter();
    bufferedWriter.write("FAIL_COUNT=0\n");
    this.sqlWriter = new StringWriter();
    this.sqlDir = Paths.get("sqlDir");
  }

  /**
   * @param spaces the spaces to add to the script
   * @return the result of the file writing
   */
  @Override
  public JobResult spaceOutput(Collection<String> spaces) {
    List<String> added = new ArrayList<>();
    JobResult result = new JobResult();
    for (String space : spaces) {
      String log = format("echo \"%nmaking space %s\"%n", space);
      bufferedWriter.write(log);
      String curlCommand =
          "curl -X POST \\\n"
              + "${DREMIO_HOST}/api/v3/catalog \\\n"
              + "  -H \"Authorization: _dremio${TOKEN}\" \\\n"
              + "  -H 'Content-Type: application/json' \\\n"
              + "  -d '{\n"
              + "    \"entityType\": \"space\",\n"
              + "    \"name\": \""
              + space
              + "\"\n"
              + "}'\n";
      bufferedWriter.write(curlCommand);
      bufferedWriter.flush();
      added.add(space);
    }
    result.setSuccess(true);
    result.added(added);
    return result;
  }

  /**
   * @param folders folders to add to the script
   * @return the result of writing to the script
   */
  @Override
  public JobResult folderOutput(Collection<Collection<String>> folders) {
    List<String> added = new ArrayList<>();
    JobResult result = new JobResult();
    for (Collection<String> folder : folders) {
      String log = format("echo \"%nmaking folder %s\"%n", join(".", folder));
      bufferedWriter.write(log);
      String curlCommand =
          "curl -X POST \\\n"
              + "${DREMIO_HOST}/api/v3/catalog \\\n"
              + "  -H \"Authorization: _dremio${TOKEN}\" \\\n"
              + "  -H 'Content-Type: application/json' \\\n"
              + "  -d '{\n"
              + "    \"entityType\": \"folder\",\n"
              + "    \"path\": ["
              + folder.stream().map(x -> format("\"%s\"", x)).collect(Collectors.joining(",\n"))
              + "]\n"
              + "}'\n";
      bufferedWriter.write(curlCommand);
      bufferedWriter.flush();
      added.add(format("[ %s ]", join(", ", folder)));
    }
    result.setSuccess(true);
    result.added(added);
    return result;
  }

  /**
   * @param pdsSql the sql to add to files in the sql directory and the curl commands that are added
   *     to the script to call them
   * @return the result of writing to the files
   */
  @Override
  public JobResult writePDSs(Collection<PdsSql> pdsSql) {
    List<String> added = new ArrayList<>();
    for (PdsSql pds : pdsSql) {
      added.add(pds.getTableName());
      Path newSqlFile = Paths.get(this.sqlDir.toString(), pds.getTableName() + ".json");
      Map<String, String> sqlMap = new HashMap<>();
      sqlMap.put("sql", pds.getSql());
      sqlWriter.append(pds.getSql());
      ObjectMapper mapper = new ObjectMapper();
      String content;
      try {
        content = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sqlMap);
        this.writeFile(newSqlFile.toString(), content.getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // we want to generate unix line endings even on windows do not use %n here
      String log = format("echo \"\nmaking pds %s\"\n", pds.getTableName().replace("\"", "\\\""));
      bufferedWriter.write(log);
      String curlCommand =
          "JOB_ID=$(curl -s -X POST "
              + "${DREMIO_HOST}/api/v3/sql "
              + "  -H \"Authorization: _dremio${TOKEN}\" "
              + "  -H 'Content-Type: application/json' "
              + "  -d '@"
              + newSqlFile
              + "' | grep id | awk -F ':\"' '{print $2}' | tr -d '\"' | tr -d '\\}')\n";
      bufferedWriter.write(curlCommand);
      bufferedWriter.write("check_job\n");
      bufferedWriter.flush();
    }
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(added);
    return result;
  }

  /**
   * @param vdsSql the sql to add to files in the sql directory and the curl commands that are added
   *     to the script to call them
   * @return result of writing to the scripts
   */
  @Override
  public JobResult writeVDSs(
      final Collection<VdsSql> vdsSql, final Collection<VdsReference> vdsReferences) {
    for (final VdsReference v : vdsReferences) {
      debugWriter.append(String.format("%s%n", v));
    }
    List<String> added = new ArrayList<>();
    for (VdsSql vds : vdsSql) {
      added.add(vds.getTableName());
      Path newSqlFile = Paths.get(this.sqlDir.toString(), vds.getTableName() + ".json");
      Map<String, String> sqlMap = new HashMap<>();
      sqlMap.put("sql", vds.getSql());
      ObjectMapper mapper = new ObjectMapper();
      try {
        String content = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sqlMap);
        this.writeFile(newSqlFile.toString(), content.getBytes(StandardCharsets.UTF_8));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      String log = format("echo \"%nmaking vds %s\"%n", vds.getTableName().replace("\"", "\\\""));
      bufferedWriter.write(log);
      for (final VdsReference v : vdsReferences) {
        if (v.getName().equals(vds.getTableName())) {
          if (!v.isValid()) {
            bufferedWriter.write(
                String.format(
                    "echo \"WARNING ATTENTION: %s is missing the following references and may fail"
                        + " [ %s ]\"%n",
                    v.getName(), String.join(", ", v.getMissingReferences())));
          }
        }
      }
      String curlCommand =
          "JOB_ID=$(curl -s -X POST "
              + "${DREMIO_HOST}/api/v3/sql "
              + "  -H \"Authorization: _dremio${TOKEN}\" "
              + "  -H 'Content-Type: application/json' "
              + "  -d '@"
              + newSqlFile
              + "' | grep id | awk -F ':\"' '{print $2}' | tr -d '\"' | tr -d '\\}')\n";
      bufferedWriter.write(curlCommand);
      bufferedWriter.write("check_job\n");
      bufferedWriter.flush();
    }
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(added);
    return result;
  }

  /**
   * @param sources sources to add to the script
   * @return the result of writing to the script file
   */
  @Override
  public JobResult sourceOutput(Collection<String> sources, Optional<String> defaultCtasFormat) {
    final List<String> added = new ArrayList<>();
    for (String source : sources) {
      added.add(source);
      String log = format("echo \"%nmaking source %s\"%n", source.replace("\"", "\\\""));
      bufferedWriter.write(log);
      try {
        bufferedWriter.write("mkdiriflocal ");
        Path newDir = this.fileMaker.getNewDir();
        bufferedWriter.write(format("%s", newDir));
        bufferedWriter.write("\n");
        bufferedWriter.write(
            "curl -s -X POST \\\n"
                + "${DREMIO_HOST}/api/v3/catalog \\\n"
                + "  -H \"Authorization: _dremio${TOKEN}\" \\\n"
                + "  -H 'Content-Type: application/json' \\\n"
                + "  -d \"{\n");

        bufferedWriter.write(
            "    \\\"metadataPolicy\\\": {        \\\"authTTLMs\\\":86400000,\n"
                + "        \\\"namesRefreshMs\\\":3600000,\n"
                + "        \\\"datasetRefreshAfterMs\\\": 3600000,\n"
                + "        \\\"datasetExpireAfterMs\\\": 10800000,\n"
                + "        \\\"datasetUpdateMode\\\":\\\"PREFETCH_QUERIED\\\",\n"
                + "        \\\"deleteUnavailableDatasets\\\": true,\n"
                + "        \\\"autoPromoteDatasets\\\": true\n"
                + "        },\n"
                + "    \\\"entityType\\\": \\\"source\\\",\n"
                + "    \\\"type\\\": \\\"NAS\\\",\n"
                + "    \\\"config\\\": {\\\"path\\\": \\\""
                + format("%s\\\"", newDir));
        defaultCtasFormat.ifPresent(
            s -> bufferedWriter.write(String.format(",\\\"defaultCtasFormat\\\":\\\"%s\\\"", s)));
        bufferedWriter.write(
            "},\n" + "    \\\"name\\\": \\\"" + source.replace("\"", "") + "\\\"\n" + "}\"\n");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      bufferedWriter.flush();
    }
    bufferedWriter.flush();
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(added);
    return result;
  }

  protected abstract void writeFile(String fileName, byte[] data) throws IOException;

  @Override
  public void close() throws IOException {
    // close out retry loop
    bufferedWriter.write("\tdone\n");
    bufferedWriter.flush();
    writeFile("create.sh", bufferedWriter.toString().getBytes(StandardCharsets.UTF_8));
    debugWriter.flush();
    writeFile("debug.log", debugWriter.toString().getBytes(StandardCharsets.UTF_8));
    sqlWriter.flush();
    writeFile("debug.sql", sqlWriter.toString().getBytes(StandardCharsets.UTF_8));
    writeFile("README.md", getREADMEText().getBytes(StandardCharsets.UTF_8));
  }

  protected String getSqlDir() {
    return sqlDir.toString();
  }

  private String getREADMEText() {
    StringBuilder builder = new StringBuilder();
    builder.append("\tUsage:\n");
    builder.append(
        "\t\t./create.sh --host http://localhost:9047 --user dremio --password" + " dremio123\n");
    builder.append("\n## Important Notes:\n");
    builder.append("* This should work on all versions of dremio\n");
    builder.append(
        "* idempotent, rerun the script as much as you want, it will skip already created"
            + " spaces, source, pds and vds\n");
    builder.append("* dates generated are from past 2 years\n");
    builder.append(
        "* if there is a problem with VDS order detection the script will try and run the"
            + " VDSs again and again until the order self corrects or the script gives up\n");
    builder.append("\n## Known Missing Features:\n");
    builder.append(
        "* does not support generating data sources from postgres/msql/etc. However, one can"
            + " create the source of the same name and rerun the script, as long as the source"
            + " supports CTAS it will work\n");
    builder.append(
        "* does not support creation of data sources on remote dremio servers (only on local"
            + " host). This can be worked around by creating the source first and as long as the"
            + " name matches and the source supports CTAS the reproduction will continue on\n");
    builder.append(
        "* does not support pre-iceberg promotion of the PDS, one will have to either query"
            + " the dataset using autopromotion or manually promote it\n");
    return builder.toString();
  }
}
