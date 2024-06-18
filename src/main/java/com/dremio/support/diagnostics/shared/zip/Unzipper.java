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
package com.dremio.support.diagnostics.shared.zip;

import com.dremio.support.diagnostics.shared.PathAndStream;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;

/** interface for unzipping logic */
public interface Unzipper {
  Collection<Extraction> unzipAllFiles(PathAndStream zipFile, Function<String, Boolean> filter)
      throws IOException;

  /**
   * takes a zip file and returns the Path to profile.json located inside
   *
   * @param zipFile zip file to search
   * @return path to profile.json to analyze
   */
  Extraction unzipProfileJSON(PathAndStream zipFile) throws IOException;
}
