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
package com.dremio.support.diagnostics.repro.fileout;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.support.diagnostics.repro.JobResult;
import com.dremio.support.diagnostics.repro.PdsSql;
import com.dremio.support.diagnostics.repro.SqlOutput;
import com.dremio.support.diagnostics.repro.VdsSql;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SqlDebugLogOutputTest {

  private static final String test = "test";
  private static final String sql = "sql";

  @Test
  void getName() throws IOException {
    String file = Files.createTempFile(test, sql).toString();
    try (SqlDebugLogOutput sqlDebug = new SqlDebugLogOutput(file)) {
      assertThat(sqlDebug.getName()).isEqualTo("SQL Debug Log " + file);
    }
  }

  static class OnlySpaceOutputIsNoOp {
    private static byte[] fileContents;
    private static JobResult jobResult;

    @BeforeAll
    static void initAll() throws IOException {
      String file = Files.createTempFile(test, sql).toString();
      try (SqlDebugLogOutput sqlDebug = new SqlDebugLogOutput(file)) {
        jobResult = sqlDebug.spaceOutput(Arrays.asList("a", "b", "c"));
      }
      fileContents = Files.readAllBytes(Paths.get(file));
    }

    @Test
    void testJobIsSuccessful() {
      assertThat(jobResult.getSuccess()).isTrue();
    }

    @Test
    void testOutputFileIsEmpty() {
      assertThat(fileContents).isEqualTo("".getBytes(StandardCharsets.UTF_8));
    }
  }

  static class SpaceAndFolderOutputIsNoOp {
    private static byte[] fileContents;
    private static JobResult jobResult;

    @BeforeAll
    static void initAll() throws IOException {
      String file = Files.createTempFile(test, sql).toString();
      try (SqlOutput sqlDebug = new SqlDebugLogOutput(file)) {
        jobResult = sqlDebug.folderOutput(Collections.singletonList(Arrays.asList("a", "b", "c")));
      }
      fileContents = Files.readAllBytes(Paths.get(file));
    }

    @Test
    void testJobIsSuccessful() throws IOException {
      assertThat(jobResult.getSuccess()).isTrue();
    }

    @Test
    void testFileIsEmpty() {
      assertThat(fileContents).isEqualTo("".getBytes(StandardCharsets.UTF_8));
    }
  }

  static class SourceOutputIsNoOp {
    private static byte[] fileContents;
    private static JobResult jobResult;

    @BeforeAll
    static void initAll() throws IOException {
      String file = Files.createTempFile(test, sql).toString();
      try (SqlOutput sqlDebug = new SqlDebugLogOutput(file)) {
        jobResult = sqlDebug.sourceOutput(Arrays.asList("a", "b", "c"), Optional.empty());
      }
      fileContents = Files.readAllBytes(Paths.get(file));
    }

    @Test
    void testJobIsSuccessful() throws IOException {
      assertThat(jobResult.getSuccess()).isTrue();
    }

    @Test
    void testFileIsEmpty() {
      assertThat(fileContents).isEqualTo("".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Nested
  static class PDSsFilesAreWrittenOut {
    private static byte[] fileContents;
    private static JobResult jobResult;

    @BeforeAll
    static void initAll() throws IOException {
      String file = Files.createTempFile(test, sql).toString();
      PdsSql pds1 = new PdsSql("a", "select * from a");
      PdsSql pds2 = new PdsSql("b", "select * from b");
      try (SqlOutput sqlDebug = new SqlDebugLogOutput(file)) {
        jobResult = sqlDebug.writePDSs(Arrays.asList(pds1, pds2));
      }
      fileContents = Files.readAllBytes(Paths.get(file));
    }

    @Test
    void testJobIsSuccessful() throws IOException {
      assertThat(jobResult.getSuccess()).isTrue();
    }

    @Test
    void testSqlFromPDSIsStored() {
      assertThat(new String(fileContents, StandardCharsets.UTF_8).replace("\r", ""))
          .isEqualTo("select * from a\nselect * from b\n");
    }
  }

  @Nested
  static class VDSsFilesAreWrittenOut {
    private static byte[] fileContents;
    private static JobResult jobResult;

    @BeforeAll
    static void initAll() throws IOException {
      String file = Files.createTempFile(test, sql).toString();
      VdsSql vds1 = new VdsSql("a", "select * from a", new String[0]);
      VdsSql vds2 = new VdsSql("b", "select * from b", new String[0]);
      try (SqlOutput sqlDebug = new SqlDebugLogOutput(file)) {
        jobResult = sqlDebug.writeVDSs(Arrays.asList(vds1, vds2), new ArrayList<>());
      }
      fileContents = Files.readAllBytes(Paths.get(file));
    }

    @Test
    void testJobIsSuccessful() throws IOException {
      assertThat(jobResult.getSuccess()).isTrue();
    }

    @Test
    void testSqlFromPDSIsStored() {
      assertThat(new String(fileContents, StandardCharsets.UTF_8).replace("\r", ""))
          .isEqualTo("select * from a\nselect * from b\n");
    }
  }
}
