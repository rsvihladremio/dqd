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
package com.dremio.support.diagnostics.shared;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * thread safe and should be shared among several instances if need be. This will create a
 * monotonically increasing directory each time one is asked for using the base directory passed for
 * mapping.
 */
public class DirectoryMaker implements FileMaker {

  private final String baseDir;
  private AtomicInteger counter = new AtomicInteger(0);

  public DirectoryMaker(String baseDir) {
    this.baseDir = baseDir;
  }

  /**
   * Makes a new directory and returns the path. Cleanup is not automatic and this is assumed to be
   * a permanent directory, it is up the consumer to remove the directory if it is not desired. This
   * is a monotonically increasing value and thread safe
   *
   * @return path to newly created subdirectory located in the base directory specified in the
   *     constructor
   */
  @Override
  public Path getNewDir() {
    Path newDir =
        Paths.get(this.baseDir, String.format("dremio-repro-source-%d", this.counter.addAndGet(1)));
    try {
      Files.createDirectories(newDir);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("unable to create directories %s", e.getMessage()), e);
    }
    return newDir;
  }
}
