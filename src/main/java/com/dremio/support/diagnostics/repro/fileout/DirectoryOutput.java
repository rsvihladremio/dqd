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

import com.dremio.support.diagnostics.shared.FileMaker;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;

public class DirectoryOutput extends FileOutput {

  private Path baseDir;

  public DirectoryOutput(Path baseDir, int timeoutSeconds, FileMaker fileMaker) throws IOException {
    super(timeoutSeconds, fileMaker);
    if (baseDir == null) {
      throw new InvalidParameterException("baseDir is not valid");
    }
    this.baseDir = baseDir;
    Files.createDirectories(this.baseDir);
    Files.createDirectories(Paths.get(this.baseDir.toString(), super.getSqlDir()));
  }

  @Override
  protected void writeFile(String fileName, byte[] data) throws IOException {
    try (OutputStream out = Files.newOutputStream(Paths.get(baseDir.toString(), fileName))) {
      out.write(data);
      out.flush();
    }
  }

  /**
   * provides a title so that consumers can know which strategy is running
   *
   * @return provides a title so that consumers can know which strategy is running
   */
  @Override
  public String getName() {
    return String.format("directory %s/sqlDir and script %s/create.sh", this.baseDir, this.baseDir);
  }
}
