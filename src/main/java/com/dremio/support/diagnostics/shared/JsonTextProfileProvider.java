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

import com.dremio.support.diagnostics.profilejson.ProfileJSONParser;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.InvalidParameterException;

/** simple case parser, just reads a json text file and converts it into a ProfileJSON */
public class JsonTextProfileProvider implements ProfileProvider {

  private final InputStream file;
  private final Path filePath;

  /**
   * @param pathAndStream stream of the json file and it's path
   */
  public JsonTextProfileProvider(final PathAndStream pathAndStream) {
    if (pathAndStream == null
        || pathAndStream.filePath() == null
        || pathAndStream.stream() == null) {
      throw new InvalidParameterException("no json file to parse critical error");
    }
    this.file = pathAndStream.stream();
    this.filePath = pathAndStream.filePath();
  }

  /**
   * @return the parsed profile
   */
  @Override
  public final ProfileJSON getProfile() throws IOException {
    ProfileJSONParser parser = new ProfileJSONParser();
    return parser.parseFile(this.file);
  }

  /**
   * @return path to the file
   */
  @Override
  public Path getFilePath() {
    return this.filePath;
  }
}
