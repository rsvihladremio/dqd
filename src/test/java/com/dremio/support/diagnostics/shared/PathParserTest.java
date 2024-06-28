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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PathParserTest {

  private static final String firstProfile = "testfile.json";
  private static final String secondProfile = "testfile2.json";

  @Nested
  static class PathParserParsingDirectoryTest {

    private static String[] files;

    @BeforeAll
    static void initAll() throws FileNotFoundException {
      PathParser parser = new PathParser();
      String path = "src/test/resources/com/dremio/support/diagnostics/testdir";

      File file = new File(path);
      String absolutePath = file.getAbsolutePath();
      String[] f = parser.convertPathToFiles(absolutePath);
      files = Arrays.stream(f).sorted().toArray(String[]::new);
    }

    @Test
    void test2FilesAreFound() {
      assertEquals(files.length, 2);
    }

    @Test
    void testFirstFileIsFound() {
      assertTrue(files[0].endsWith(firstProfile));
    }

    @Test
    void testSecondFileIsFound() {
      assertTrue(files[1].endsWith(secondProfile));
    }
  }

  @Nested
  static class PathParserParsingCommaSeparatedListOfFilesTest {
    private static String[] files;

    @BeforeAll
    static void initAll() throws FileNotFoundException {
      PathParser parser = new PathParser();
      String path =
          "src/test/resources/com/dremio/support/diagnostics/testdir/testfile.json,src/test/resources/com/dremio/support/diagnostics/testdir/testfile2.json";

      File file = new File(path);
      String absolutePath = file.getAbsolutePath();
      String[] f = parser.convertPathToFiles(absolutePath);
      files = Arrays.stream(f).sorted().toArray(String[]::new);
    }

    @Test
    void testThereAre2Files() throws FileNotFoundException {
      assertEquals(files.length, 2);
    }

    @Test
    void testFirstFileIsFound() {
      assertTrue(files[0].endsWith(firstProfile));
    }

    @Test
    void testSecondFileIsFound() {
      assertTrue(files[1].endsWith(secondProfile));
    }
  }

  @Nested
  static class PathParserWithBadFileInCommaSeparatedListTest {

    @Test
    void testUnknownFilesResultInAnException() {
      try {
        new PathParser()
            .convertPathToFiles(
                new File(
                        "src/test/resources/com/dremio/support/diagnostics/testdir/testfile.json,NOWHEREVALID")
                    .getAbsolutePath());
        assertFalse(true, "we should not hit this it means there was no exception");
      } catch (FileNotFoundException ex) {
        assertTrue(ex.getMessage().contains("NOWHEREVALID"));
      }
    }
  }

  @Nested
  static class PathParserWithSingleFile {
    private static String[] files;

    @BeforeAll
    static void initAll() throws FileNotFoundException {
      PathParser parser = new PathParser();
      String path = "src/test/resources/com/dremio/support/diagnostics/testdir/testfile.json";

      File file = new File(path);
      String absolutePath = file.getAbsolutePath();
      files = parser.convertPathToFiles(absolutePath);
    }

    @Test
    void testOnlyOneFileFound() {
      assertEquals(files.length, 1);
    }

    @Test
    void testItIsTheCorrectFile() {
      assertTrue(files[0].endsWith(firstProfile));
    }
  }

  @Nested
  static class PathParserWhenSingleFileUsedIsNotFoundTest {
    @Test
    void testThrowsException() {
      try {
        new PathParser().convertPathToFiles(new File("NOWHEREVALID").getAbsolutePath());
        assertFalse(true, "we should not hit this it means there was no exception");
      } catch (FileNotFoundException ex) {
        assertTrue(ex.getMessage().contains("NOWHEREVALID"));
      }
    }
  }
}
