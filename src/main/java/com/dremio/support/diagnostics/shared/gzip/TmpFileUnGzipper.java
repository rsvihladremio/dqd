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

import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.zip.Extraction;
import com.dremio.support.diagnostics.shared.zip.TmpFileExtraction;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class TmpFileUnGzipper implements UnGzipper {
  Random random = new Random();
  int bufferSize = 8192;

  private static final Logger logger = Logger.getLogger(TmpFileUnGzipper.class.getName());

  @Override
  public Extraction unGzipFile(final PathAndStream gzipFile) throws IOException {

    var tmpDir = Files.createTempDirectory("dqd-zip-%d".formatted(random.nextInt(0, 1000000)));
    final String rawFileName = gzipFile.filePath().getFileName().toString();
    final String pathFileName;
    final String fileName;
    if (rawFileName.endsWith(".gz")) {
      fileName = rawFileName.substring(0, rawFileName.length() - 3);
    } else {
      fileName = rawFileName;
    }
    pathFileName = Paths.get(tmpDir.toString(), fileName).toString();
    try (final GzipCompressorInputStream gis = new GzipCompressorInputStream(gzipFile.stream())) {
      try (final FileOutputStream outputStream = new FileOutputStream(pathFileName)) {
        final byte[] buffer = new byte[bufferSize];
        int totalRead = 0;
        int read;
        while ((read = gis.read(buffer)) != -1) {
          outputStream.write(buffer, 0, read);
          totalRead += read;
        }

        if (logger.isLoggable(Level.INFO)) {
          final String bytesRead = Human.getHumanBytes1000(totalRead);
          logger.info(() -> String.format("%s read of gzipped file %s", bytesRead, fileName));
        }
      }
      return new TmpFileExtraction(
          tmpDir,
          Collections.singletonList(
              new PathAndStream(Paths.get(fileName), new FileInputStream(pathFileName))));
    }
  }
}
