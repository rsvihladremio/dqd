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
package com.dremio.support.diagnostics.shared.gzip;

import static com.dremio.support.diagnostics.FileTestHelpers.readAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.zip.Extraction;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import org.junit.jupiter.api.Test;

class TmpFileUnGzipperTest {
  @Test
  void testExtractsUnGzipWithCorrectName() throws IOException {
    final TmpFileUnGzipper unzipper = new TmpFileUnGzipper();
    try (final Extraction result =
        unzipper.unGzipFile(FileTestHelpers.getTestQueriesJsonInGunzip())) {
      final Collection<PathAndStream> pathAndStreamCollection = result.getPathAndStreams();
      assertEquals(pathAndStreamCollection.size(), 1, "expected only one file in gzip");
      final Path filePath = result.getPathAndStreams().iterator().next().filePath();
      assertEquals(
          Paths.get("queries.json"), filePath, "not the correct file path for an ungzipped file");
    }
  }

  @Test
  void testExtractsUnGzipWithValidFile() throws IOException {
    final TmpFileUnGzipper unzipper = new TmpFileUnGzipper();
    try (final Extraction result =
        unzipper.unGzipFile(FileTestHelpers.getTestQueriesJsonInGunzip())) {
      final Collection<PathAndStream> pathAndStreamCollection = result.getPathAndStreams();
      assertEquals(pathAndStreamCollection.size(), 1, "expected only one file in gzip");
      final InputStream inputStream = result.getPathAndStreams().iterator().next().stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStream),
          "was not the expected file extracted from the gunzip");
    }
  }
}
