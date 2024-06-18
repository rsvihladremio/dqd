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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.logging.Logger;

public class TmpFileExtraction extends Extraction {
  private final Path extractedDir;
  private static final Logger logger = Logger.getLogger(TmpFileExtraction.class.getName());

  public TmpFileExtraction(Path extractedDir, Collection<PathAndStream> pathAndStreams) {
    super(pathAndStreams);
    this.extractedDir = extractedDir;
  }

  @Override
  public void close() {
    for (var pathAndStream : getPathAndStreams()) {
      if (pathAndStream.stream() != null) {
        try {
          pathAndStream.stream().close();
        } catch (IOException e) {
          logger.warning(
              () ->
                  "unable to close stream for file %s with error %s"
                      .formatted(pathAndStream.filePath(), e.getMessage()));
        }
      }
    }
    if (this.extractedDir != null) {
      try {
        try (var stream = Files.walk(this.extractedDir).sorted(Comparator.reverseOrder())) {
          stream.forEach(
              x -> {
                try {
                  Files.delete(x);
                } catch (IOException e) {
                  logger.warning(
                      () ->
                          "unable to cleanup %s due to error %s will need to remove it manually"
                              .formatted(x, e.getMessage()));
                }
              });
        }
      } catch (IOException e) {
        logger.warning(
            () ->
                "unable to cleanup %s due to error %s will need to remove it manually"
                    .formatted(this.extractedDir, e.getMessage()));
      }
    }
  }
}
