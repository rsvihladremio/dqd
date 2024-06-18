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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DirectoryMakerTest {

  private static Path newDir;
  private static Path newDir2;

  @BeforeAll
  static void initAll() throws IOException {
    final Path tmpDir = Files.createTempDirectory("tester");
    DirectoryMaker maker = new DirectoryMaker(tmpDir.toString());
    newDir = maker.getNewDir();
    newDir2 = maker.getNewDir();
  }

  @Test
  void testMakeDirectorySucceedsWith1AsNumber() {
    assertThat(newDir.endsWith("dremio-repro-source-1")).isTrue();
  }

  @Test
  void testNumberIncrements() {
    assertThat(newDir2.endsWith("dremio-repro-source-2")).isTrue();
  }
}
