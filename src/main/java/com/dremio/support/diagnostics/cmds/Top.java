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

import com.dremio.support.diagnostics.top.TopExec;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Top implements Callable<Integer> {

  /**
   * the primary file to analyze
   */
  @CommandLine.Parameters(
      index = "0",
      description = "top file generated with \"LINES=100 top -H -n 120 -p 1 -d 2 -bw\"")
  private File file;

  @Option(
      names = {"-o", "--output"},
      defaultValue = "top.html",
      description = "location to print the report out to")
  private String reportOutputPath;

  @Override
  public Integer call() throws Exception {
    try (final OutputStream outputStream = Files.newOutputStream(Paths.get(reportOutputPath))) {
      try (final InputStream inputStream = Files.newInputStream(file.toPath())) {
        TopExec.exec(inputStream, outputStream);
      }
    }
    return 0;
  }
}
