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

import static com.dremio.support.diagnostics.FileTestHelpers.readAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.google.common.collect.Iterables;
import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class UnzipperImplTest {
  private static final Path expectedProfile = Paths.get("profile_attempt_0.json");
  private static final Path expectedQueriesJson = Paths.get("queries.json");

  private void extractResource(String resourcePath, Consumer<Collection<PathAndStream>> exec)
      throws IOException {
    final var extractions = getAllFiles(resourcePath);
    final List<PathAndStream> pathAndStreams = new ArrayList<>();
    for (Extraction e : extractions) {
      pathAndStreams.addAll(e.getPathAndStreams());
    }
    try {
      exec.accept(pathAndStreams);
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  @Test
  void testUnzipsFile() throws IOException {
    final PathAndStream pathAndStream = getProfile("/testprofile.zip");
    final Path filePath = getFilePath(pathAndStream);
    assertEquals(
        expectedProfile, filePath.getFileName(), "does not contain profilejson in testprofile.zip");
  }

  @Test
  void testUnzipsTgz() throws IOException {
    final PathAndStream pathAndStream = getProfile("/testprofile.tgz");
    final Path filePath = getFilePath(pathAndStream);
    assertEquals(
        expectedProfile, filePath.getFileName(), "does not contain profilejson in testprofile.tgz");
  }

  @Test
  void testUnzipsTarGz() throws IOException {
    final PathAndStream pathAndStream = getProfile("/testprofile.tar.gz");
    final Path filePath = getFilePath(pathAndStream);
    assertEquals(
        expectedProfile,
        filePath.getFileName(),
        "does not contain profilejson in testprofile.tar.gz");
  }

  @Test
  void testUnzipsTarGzWithNestedGz() throws IOException {
    extractResource(
        "/queries.json.tar.gz",
        pathAndStreams -> {
          final PathAndStream firstFile = getFirst(pathAndStreams);
          final Path filePath = getFilePath(firstFile);
          assertEquals(
              expectedQueriesJson,
              filePath,
              "does not contain queries.json in queries.json.tar.gz");
        });
  }

  @Test
  void testUnzipsTarGzWithNestedGzIsValid() throws IOException {
    extractResource(
        "/queries.json.tar.gz",
        pathAndStreams -> {
          final PathAndStream firstFile = getFirst(pathAndStreams);
          final InputStream inputStream = firstFile.stream();
          try {
            assertEquals(
                FileTestHelpers.getTestQueriesJsonText(),
                readAll(inputStream),
                "does not contain queries.json in queries.json.tar.gz");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  void testUnzipsTarGzWithNestedTarGzIsValid() throws IOException {
    extractResource(
        "/queries.json.tgz.tgz",
        pathAndStreams -> {
          final PathAndStream firstFile = getFirst(pathAndStreams);
          final InputStream inputStream = firstFile.stream();
          try {
            assertEquals(
                FileTestHelpers.getTestQueriesJsonText(),
                readAll(inputStream),
                "does not contain queries.json in queries.json.tgz.tgz");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  void testUnzipsTgzWithNestedGz() throws IOException {
    extractResource(
        "/queries.json.tgz",
        pathAndStreams -> {
          final PathAndStream firstFile = getFirst(pathAndStreams);
          final Path filePath = getFilePath(firstFile);
          assertEquals(
              expectedQueriesJson, filePath, "does not contain queries.json in queries.json.tgz");
        });
  }

  @Test
  void testUnzipsTgzWithNestedGzIsHasValidPath() throws IOException {
    extractResource(
        "/queries.json.tgz",
        pathAndStreams -> {
          final PathAndStream firstFile = getFirst(pathAndStreams);
          final Path filePath = getFilePath(firstFile);
          assertEquals(
              expectedQueriesJson, filePath, "does not contain queries.json in queries.json.tgz");
        });
  }

  @Test
  void testUnzipsTgzWithNestedGzIsValid() throws IOException {
    final var extractions = getAllFiles("/queries.json.tgz");
    try {
      final List<PathAndStream> pathAndStreams = new ArrayList<>();
      for (Extraction e : extractions) {
        pathAndStreams.addAll(e.getPathAndStreams());
      }
      final PathAndStream firstFile = getFirst(pathAndStreams);
      final InputStream inputStream = firstFile.stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStream),
          "does not contain queries.json in queries.json.tar.gz");
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  @Test
  void testUnzipsGz() throws IOException {
    final var extractions = getAllFiles("/queries.json.gz");
    try {
      final List<PathAndStream> pathAndStreams = new ArrayList<>();
      for (Extraction e : extractions) {
        pathAndStreams.addAll(e.getPathAndStreams());
      }
      final PathAndStream firstFile = getFirst(pathAndStreams);
      final Path filePath = getFilePath(firstFile);
      assertEquals(
          expectedQueriesJson, filePath, "does not contain queries.json in queries.json.gz");
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  @Test
  void testUnzipsGzIsValid() throws IOException {
    final var extractions = getAllFiles("/queries.json.gz");
    try {
      final List<PathAndStream> pathAndStreams = new ArrayList<>();
      for (Extraction e : extractions) {
        pathAndStreams.addAll(e.getPathAndStreams());
      }
      final PathAndStream firstFile = getFirst(pathAndStreams);
      final InputStream inputStream = firstFile.stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStream),
          "does not contain queries.json in queries.json.tar.gz");
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  @Test
  void testUnzipTgzLotsOfNestedGzs() throws IOException {
    final var extractions = getAllFiles("/big-queries.json.tgz");
    final Collection<PathAndStream> pathAndStreams = new ArrayList<>();
    for (Extraction e : extractions) {
      pathAndStreams.addAll(e.getPathAndStreams());
    }
    try {
      final PathAndStream firstFile = getFirst(pathAndStreams);
      final InputStream inputStream = firstFile.stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStream),
          "does not contain queries.json in queries.json.tar.gz");
      final InputStream inputStreamNumber2 = Iterables.get(pathAndStreams, 1).stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStreamNumber2),
          "does not contain queries.json in queries.json.tar.gz");
      final InputStream inputStreamNumber3 = Iterables.get(pathAndStreams, 2).stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStreamNumber3),
          "does not contain queries.json in queries.json.tar.gz");
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  @Test
  void testUnzipZipLotsOfQueriesFiles() throws IOException {
    final Collection<Extraction> extractions = getAllFiles("/big-queries-unzipped.json.zip");
    try {
      final Collection<PathAndStream> pathAndStreams = new ArrayList<>();
      for (Extraction e : extractions) {
        pathAndStreams.addAll(e.getPathAndStreams());
      }
      final PathAndStream firstFile = getFirst(pathAndStreams);
      final InputStream inputStream = firstFile.stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStream),
          "does not contain 1st queries.json in big-queries-unzipped.json.zip");
      final InputStream inputStreamNumber2 = Iterables.get(pathAndStreams, 1).stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStreamNumber2),
          "does not contain 2nd queries.json in big-queries-unzipped.json.zip");
      final InputStream inputStreamNumber3 = Iterables.get(pathAndStreams, 2).stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStreamNumber3),
          "does not contain 3rd queries.json in big-queries-unzipped.json.zip");
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  @Test
  void testUnzipZipLotsOfNestedGzs() throws IOException {
    final Collection<Extraction> extractions = getAllFiles("/big-queries.json.zip");
    try {
      final Collection<PathAndStream> pathAndStreams = new ArrayList<>();
      for (Extraction e : extractions) {
        pathAndStreams.addAll(e.getPathAndStreams());
      }
      final PathAndStream firstFile = getFirst(pathAndStreams);
      final InputStream inputStream = firstFile.stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStream),
          "does not contain 1st queries.json in big-queries.json.zip");
      final InputStream inputStreamNumber2 = Iterables.get(pathAndStreams, 1).stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStreamNumber2),
          "does not contain 2nd queries.json in big-queries.json.zip");
      final InputStream inputStreamNumber3 = Iterables.get(pathAndStreams, 2).stream();
      assertEquals(
          FileTestHelpers.getTestQueriesJsonText(),
          readAll(inputStreamNumber3),
          "does not contain 3rd queries.json in big-queries.json.zip");
    } finally {
      for (Extraction extraction : extractions) {
        extraction.close();
      }
    }
  }

  private void validateResource(final URL resource) {
    if (resource == null) {
      fail("unexpected null test file");
    }
  }

  private Collection<Extraction> getAllFiles(final String resourcePath) throws IOException {
    final URL resource = this.getClass().getResource(resourcePath);
    validateResource(resource);
    try (FileInputStream fs = new FileInputStream(Objects.requireNonNull(resource).getFile())) {
      final UnzipperImpl unzipper = new UnzipperImpl();
      return unzipper.unzipAllFiles(
          new PathAndStream(Paths.get(resource.getPath()), fs), x -> true);
    }
  }

  private PathAndStream getProfile(final String resourcePath) throws IOException {
    final URL resource = this.getClass().getResource(resourcePath);
    if (resource == null) {
      throw new RuntimeException("unable to read profile at %s".formatted(resourcePath));
    }
    validateResource(resource);
    try (FileInputStream fs = new FileInputStream(resource.getFile())) {
      final UnzipperImpl unzipper = new UnzipperImpl();
      try (final var extraction =
          unzipper.unzipProfileJSON(new PathAndStream(Paths.get(resource.getPath()), fs))) {
        var pathAndStreams = extraction.getPathAndStreams();
        if (pathAndStreams == null) {
          throw new RuntimeException("no profile.json in file %s".formatted(resourcePath));
        }
        if (pathAndStreams.size() != 1) {
          throw new RuntimeException(
              "expected 1 profile.json but had %d".formatted(pathAndStreams.size()));
        }
        return pathAndStreams.iterator().next();
      }
    }
  }

  private Path getFilePath(final PathAndStream pathAndStream) {
    return pathAndStream.filePath();
  }

  @SuppressWarnings("null")
  private PathAndStream getFirst(final Collection<PathAndStream> pathAndStreams) {
    return Iterables.getFirst(pathAndStreams, null);
  }
}
