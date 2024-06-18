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
import java.io.InputStream;
import java.nio.file.Path;
import java.util.logging.Logger;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class ArchiveDetection {

  private static final Logger logger = Logger.getLogger(ArchiveDetection.class.getName());

  @SuppressWarnings("rawtypes")
  public static ArchiveInputStream getArchive(final PathAndStream pathAndStream)
      throws IOException {
    final Path filePath = pathAndStream.filePath();
    if (hasExtension(filePath, ".zip")) {
      logger.fine(() -> "opening zip %s".formatted(filePath));
      return new ZipArchiveInputStream(getInputStream(pathAndStream));
    } else if (hasExtension(filePath, ".tgz") || hasExtension(filePath, ".tar.gz")) {
      logger.fine(() -> "opening tar gunzip combo %s".formatted(filePath));
      final InputStream gzipStream = new GzipCompressorInputStream(getInputStream(pathAndStream));
      return new TarArchiveInputStream(gzipStream);
    } else if (hasExtension(filePath, ".tar")) {
      logger.fine(() -> "opening tar %s".formatted(filePath));
      return new TarArchiveInputStream(getInputStream(pathAndStream));
    }
    throw new IOException(
        String.format("unknown archive type %s", getExtension(getFilePathStr(filePath))));
  }

  private static boolean hasExtension(final Path fileName, final String ext) {
    final String fileNameStr = fileName.toString();
    return endsWith(fileNameStr, ext);
  }

  private static boolean endsWith(final String fileNameStr, final String ext) {
    return fileNameStr.endsWith(ext);
  }

  private static InputStream getInputStream(final PathAndStream pathAndStream) {
    return pathAndStream.stream();
  }

  public static boolean isArchive(String fileName) {
    for (final String ext : archiveExtensions) {
      if (fileName.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  public static String getFilePathStr(final Path path) {
    return path.toString();
  }

  private static final String[] archiveExtensions =
      new String[] {".zip", ".tgz", ".tar.gz", ".tar", ".gz"};

  public static boolean isCompressed(String fileName) {
    return fileName.endsWith(".gz") && !fileName.endsWith(".tar.gz");
  }

  private static String getExtension(final String fileName) {
    final int indexOf = fileName.lastIndexOf(".");
    final int notFound = -1;
    if (indexOf == notFound) {
      return "";
    }
    return fileName.substring(indexOf);
  }

  public static Path getFilePath(PathAndStream pathAndStream) {
    return pathAndStream.filePath();
  }

  public static boolean isProfileJson(final String fileNameStr) {
    // we need to allow the archives as they will get unarchived further
    return (fileNameStr.contains(".json") && fileNameStr.startsWith("profile"))
        || isArchive(fileNameStr);
  }
}
