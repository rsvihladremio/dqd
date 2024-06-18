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

import com.dremio.support.diagnostics.profilejson.Parser;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.dremio.support.diagnostics.shared.zip.Extraction;
import com.dremio.support.diagnostics.shared.zip.Unzipper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.InvalidParameterException;

public class ZipProfileProvider implements ProfileProvider {

  private final Parser parser;
  private final Unzipper unzipper;
  private final PathAndStream file;

  /**
   * @param parser the parser strategy used to process files
   * @param unzipper The logical to unzip files and iterate through entries
   * @param pathAndStream location of the zip file and it's stream
   */
  public ZipProfileProvider(
      final Parser parser, final Unzipper unzipper, final PathAndStream pathAndStream) {
    this.parser = parser;
    this.unzipper = unzipper;
    if (pathAndStream == null
        || pathAndStream.stream() == null
        || pathAndStream.filePath() == null) {
      throw new InvalidParameterException("pathAndStream cannot be null, missing file to parse");
    }
    this.file = pathAndStream;
  }

  /**
   * will open the zip parse the file and then delete the newly extracted file
   *
   * @return the parsed profile json
   */
  @Override
  public ProfileJSON getProfile() throws IOException {
    try (Extraction extraction = this.unzipper.unzipProfileJSON(this.file)) {
      if (extraction == null || extraction.getPathAndStreams() == null) {
        throw new RuntimeException(
            "unable to extract zip %s due to no value being unzipped".formatted(this.file));
      }
      if (extraction.getPathAndStreams().size() != 1) {
        throw new RuntimeException(
            "expected one profile json but had %d items. Can't proceed"
                .formatted(extraction.getPathAndStreams().size()));
      }
      try (InputStream is = extraction.getPathAndStreams().iterator().next().stream()) {
        return this.parser.parseFile(is);
      }
    }
  }

  /**
   * @return path to the zip
   */
  @Override
  public Path getFilePath() {
    return this.file.filePath();
  }
}
