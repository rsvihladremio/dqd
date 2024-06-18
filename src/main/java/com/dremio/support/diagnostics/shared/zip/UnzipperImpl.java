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

import static com.dremio.support.diagnostics.shared.zip.ArchiveDetection.*;

import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.gzip.TmpFileUnGzipper;
import com.dremio.support.diagnostics.shared.gzip.UnGzipper;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class UnzipperImpl implements Unzipper {
  private final UnGzipper unGzipper = new TmpFileUnGzipper();
  private static final Logger logger = Logger.getLogger(UnzipperImpl.class.getName());

  @Override
  public Collection<Extraction> unzipAllFiles(
      PathAndStream zipFile, Function<String, Boolean> filter) throws IOException {
    List<Extraction> extractions = new ArrayList<>();
    List<PathAndStream> streams = new ArrayList<>();
    var tempDir = Files.createTempDirectory("dqd-extract-simple");
    if (zipFile == null) {
      throw new RuntimeException("cannot use a null zip file");
    }
    if (zipFile.filePath() == null) {
      throw new RuntimeException("cannot have a null file path");
    }
    if (isCompressed(zipFile.filePath().toString())) {
      logger.fine(() -> "opening %s".formatted(zipFile.filePath()));
      final Extraction extraction = this.unGzipper.unGzipFile(zipFile);
      if (extraction == null) {
        throw new RuntimeException("invalid gzip from file %s".formatted(zipFile.filePath()));
      }
      if (extraction.getPathAndStreams() == null) {
        throw new RuntimeException(
            "invalid gzip, no files extracted from file %s".formatted(zipFile.filePath()));
      }
      extractions.add(extraction);
      for (PathAndStream pathAndStream : extraction.getPathAndStreams()) {
        if (pathAndStream == null) {
          throw new RuntimeException(
              "invalid files in extraction of file %s".formatted(zipFile.filePath()));
        }
        if (pathAndStream.filePath() == null) {
          throw new RuntimeException(
              "invalid file path critical error while extracting file %s"
                  .formatted(zipFile.filePath()));
        }
        if (isArchive(pathAndStream.filePath().toString())) {
          extractions.addAll(unzipAllFiles(pathAndStream, filter));
        } else {
          if (!filter.apply(pathAndStream.filePath().toString())) {
            logger.info(
                () ->
                    String.format(
                        "skipping %s as it did not match our file filter",
                        pathAndStream.filePath().toString()));
            continue;
          }
          streams.add(pathAndStream);
        }
      }
      return extractions;
    }
    try (@SuppressWarnings("rawtypes")
        ArchiveInputStream i = getArchive(zipFile)) {
      ArchiveEntry entry = null;
      while ((entry = i.getNextEntry()) != null) {
        if (!i.canReadEntryData(entry)) {
          // log something?
          continue;
        }

        String name = Paths.get(tempDir.toString(), entry.getName()).toString();
        File f = new File(name);
        if (entry.isDirectory()) {
          if (!f.isDirectory() && !f.mkdirs()) {
            throw new IOException("failed to create directory " + f);
          }
        } else {
          File parent = f.getParentFile();
          if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("failed to create directory " + parent);
          }
          if (!filter.apply(entry.getName())) {
            final var entry1 = entry;
            logger.info(
                () ->
                    String.format(
                        "skipping %s as it did not match our file filter", entry1.getName()));
            continue;
          }
          // write out the file output stream so we get it out of memory
          try (OutputStream o = Files.newOutputStream(f.toPath())) {
            IOUtils.copy(i, o);
          }
          var fileInputStream = Files.newInputStream(f.toPath());
          var pathAndStream = new PathAndStream(f.toPath(), fileInputStream);
          if (isCompressed(f.getName())) {
            try {
              final Extraction extraction = this.unGzipper.unGzipFile(pathAndStream);
              extractions.add(extraction);
              final Collection<PathAndStream> uncompressedPathAndStreams =
                  extraction.getPathAndStreams();
              if (uncompressedPathAndStreams.size() != 1) {
                throw new RuntimeException(
                    "should be one uncompressed stream but there was %d, exiting extraction"
                        .formatted(uncompressedPathAndStreams.size()));
              }
              final PathAndStream uncompressedPathAndStream =
                  uncompressedPathAndStreams.stream().findFirst().get();
              final Path uncompressedPath = getFilePath(uncompressedPathAndStream);
              if (isArchive(getFilePathStr(uncompressedPath))) {
                logger.info(() -> "is nested archive file: %s".formatted(f.getName()));
                extractions.addAll(unzipAllFiles(uncompressedPathAndStream, filter));
              }
            } catch (Exception ex) {
              logger.warning(
                  "unable to read file %s due to %s"
                      .formatted(f.getAbsoluteFile(), ex.getMessage()));
            }
          } else if (isArchive(f.getName())) {
            logger.info(() -> "is archive: %s".formatted(f.getName()));
            extractions.addAll(unzipAllFiles(pathAndStream, filter));
          } else if (filter.apply(f.getName())) {
            logger.info(() -> "is normal file: %s".formatted(f.getName()));
            streams.add(pathAndStream);
          } else {
            fileInputStream.close();
          }
        }
      }
    }
    var tmpFileExtract = new TmpFileExtraction(tempDir, streams);
    extractions.add(tmpFileExtract);
    return extractions;
  }

  /**
   * takes a zip file and returns the Path to profile.json located inside
   *
   * @param zipFile zip file to search
   * @return path to profile.json to analyze
   */
  @Override
  public Extraction unzipProfileJSON(PathAndStream zipFile) throws IOException {
    final var extractions = unzipAllFiles(zipFile, ArchiveDetection::isProfileJson);
    if (extractions == null) {
      throw new RuntimeException("invalid profile.json unable to extract anything");
    }
    if (extractions.size() != 1) {
      throw new RuntimeException(
          "invalid file for profile.json extraction, expected 1 file but had %d files"
              .formatted(extractions.size()));
    }
    return extractions.iterator().next();
  }
}
