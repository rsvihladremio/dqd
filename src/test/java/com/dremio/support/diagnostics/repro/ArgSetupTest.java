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
package com.dremio.support.diagnostics.repro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.repro.consoleout.ConsoleOutput;
import com.dremio.support.diagnostics.repro.fileout.SqlDebugLogOutput;
import com.dremio.support.diagnostics.repro.fileout.ZipFileOutput;
import com.dremio.support.diagnostics.repro.parse.ColumnDefYaml;
import com.dremio.support.diagnostics.repro.parse.ReproProfileParserImpl;
import com.dremio.support.diagnostics.shared.JsonTextProfileProvider;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.ZipProfileProvider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ArgSetupTest {

  private static final int timeoutSeconds = 1;
  private static final String username = "user";
  private static final String password = "pass";

  @Nested
  static class GetSqlOutputWithJustOutputZip {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      String username = "";
      String password = "";
      String host = "";
      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream)) {
          output =
              ArgSetup.getSqlOutput(
                  username, password, host, zipOutputStream, "", "", timeoutSeconds, "", false);
        }
      }
    }

    @Test
    void testOnlyOneOutput() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testHasFileOutput() {
      assertThat(output[0].getClass()).isEqualTo(ZipFileOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithJustDebugDir {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      String username = "";
      String password = "";
      String host = "";
      String sqlFile = String.valueOf(Files.createTempFile("test", "sql"));
      output =
          ArgSetup.getSqlOutput(
              username, password, host, null, null, sqlFile, timeoutSeconds, "", false);
    }

    @Test
    void testHasOnlyOneOutput() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testHasSqlDebugOutput() {
      assertThat(output[0].getClass()).isEqualTo(SqlDebugLogOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithApiMissingPassword {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      output =
          ArgSetup.getSqlOutput(
              username, "", "http://localhost:9048", null, "", "", timeoutSeconds, "", false);
    }

    @Test
    void testOnlyHas1Element() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testOnlyHasConsoleOutput() {
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithApiMissingUsername {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      output =
          ArgSetup.getSqlOutput(
              "", password, "http://localhost:9048", null, null, "", timeoutSeconds, "", false);
    }

    @Test
    void testOnlyHas1Element() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testOnlyHasConsoleOutput() {
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithApiMissingHost {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      output =
          ArgSetup.getSqlOutput(username, password, "", null, null, "", timeoutSeconds, "", false);
    }

    @Test
    void testOnlyHas1Element() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testOnlyHasConsoleOutput() {
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithApiNullUsername {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      output =
          ArgSetup.getSqlOutput(
              null, password, "http://localhost:9047", null, null, "", timeoutSeconds, "", false);
    }

    @Test
    void testOnlyHas1Element() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testOnlyHasConsoleOutput() {
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithApiNullPassword {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      output =
          ArgSetup.getSqlOutput(
              username, null, "http://localhost:9047", null, null, "", timeoutSeconds, "", false);
    }

    @Test
    void testOnlyHas1Element() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testOnlyHasConsoleOutput() {
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }
  }

  @Nested
  static class GetSqlOutputWithApiNullHost {
    private static SqlOutput[] output;

    @BeforeAll
    static void initAll() throws IOException {
      output =
          ArgSetup.getSqlOutput(
              username, password, null, null, null, "", timeoutSeconds, "", false);
      assertThat(output.length).isEqualTo(1);
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }

    @Test
    void testOnlyHas1Element() {
      assertThat(output.length).isEqualTo(1);
    }

    @Test
    void testOnlyHasConsoleOutput() {
      assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
    }
  }

  @Test
  void testGetSqlOutputWithNoFlagsHas1Output() throws IOException {
    SqlOutput[] output =
        ArgSetup.getSqlOutput("", "", "", null, null, "", timeoutSeconds, "", false);
    assertThat(output.length).isEqualTo(1);
  }

  @Test
  void testGetSqlOutputWithNoFlagsHasConsoleOutput() throws IOException {
    SqlOutput[] output =
        ArgSetup.getSqlOutput("", "", "", null, null, "", timeoutSeconds, "", false);
    assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
  }

  @Test
  void testGetSqlOutputWithNullOutputHas1Output() throws IOException {
    SqlOutput[] output =
        ArgSetup.getSqlOutput("", "", "", null, null, "", timeoutSeconds, "", false);
    assertThat(output.length).isEqualTo(1);
  }

  @Test
  void testGetSqlOutputWithNullOutputHasConsoleOutput() throws IOException {
    SqlOutput[] output =
        ArgSetup.getSqlOutput("", "", "", null, null, "", timeoutSeconds, "", false);
    assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
  }

  @Test
  void testGetSqlDebugWithNullOutputHas1Output() throws IOException {
    SqlOutput[] output =
        ArgSetup.getSqlOutput("", "", "", null, null, null, timeoutSeconds, "", false);
    assertThat(output.length).isEqualTo(1);
  }

  @Test
  void testGetSqlDebugWithNullOutputHasConsoleOutput() throws IOException {
    SqlOutput[] output =
        ArgSetup.getSqlOutput("", "", "", null, null, null, timeoutSeconds, "", false);
    assertThat(output[0].getClass()).isEqualTo(ConsoleOutput.class);
  }

  @Test
  void testGetReproProfileIsNotNull() {
    assertThat(ArgSetup.getReproProfile(200, new ColumnDefYaml())).isNotNull();
  }

  @Test
  void testGetReproProfileIsReproImpl() {
    assertThat(ArgSetup.getReproProfile(200, new ColumnDefYaml()).getClass())
        .isEqualTo(ReproProfileParserImpl.class);
  }

  static class GetProfileProviderZip {
    private static ProfileProvider profileProvider;

    @BeforeAll
    static void initAll() throws IOException {
      Path filePath =
          Paths.get(
              Objects.requireNonNull(
                      ClassLoader.getSystemClassLoader().getResource("testprofile.zip"))
                  .getPath());
      try (InputStream stream = Files.newInputStream(filePath)) {
        profileProvider = ArgSetup.getProfileProvider(new PathAndStream(filePath, stream));
      }
    }

    @Test
    void testIsNotNull() {
      assertThat(profileProvider).isNotNull();
    }

    @Test
    void testGetProfileProviderZip() {
      assertThat(profileProvider.getClass()).isEqualTo(ZipProfileProvider.class);
    }
  }

  static class GetProfileProviderText {
    private static ProfileProvider profileProvider;

    @BeforeAll
    static void initAll() throws IOException {
      PathAndStream file = FileTestHelpers.getTestProfile1();
      try {
        profileProvider = ArgSetup.getProfileProvider(file);
      } finally {
        file.stream().close();
      }
    }

    @Test
    void testGetProfileProviderText() throws IOException {
      assertThat(profileProvider.getClass()).isEqualTo(JsonTextProfileProvider.class);
    }

    @Test
    void testIsNotNull() throws IOException {
      assertThat(profileProvider).isNotNull();
    }
  }

  @Test
  void testGetProfileProviderNullFile() {
    assertThatThrownBy(() -> ArgSetup.getProfileProvider(null))
        .hasMessageContaining("no file provided to parse");
  }
}
