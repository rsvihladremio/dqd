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

import java.io.InputStream;
import java.nio.file.Path;

public record PathAndStream(Path filePath, InputStream stream) {
  /**
   * PathAndStream is an abstraction heavily used throughout the codebase, this allows passing
   * around and already parsed Stream (maybe in memory maybe on the filesystem) that we can process
   * when we feel the need and the filePath is retained for determining file type and therfore
   * changing the decision tree of how the file is processed.
   *
   * @param filePath path to the file, useful for making decisions based on file name or location
   * @param stream   actual stream of the file useful for actually reading data
   */
  public PathAndStream {}

  /**
   * getter for the InputStream
   *
   * @return actual stream of the file useful for actually reading data
   */
  @Override
  public InputStream stream() {
    return stream;
  }

  /**
   * getter for the file path
   *
   * @return path to the file, useful for making decisions based on file name or location
   */
  @Override
  public Path filePath() {
    return filePath;
  }
}
