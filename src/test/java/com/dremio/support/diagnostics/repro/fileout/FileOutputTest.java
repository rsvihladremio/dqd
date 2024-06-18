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

import com.dremio.support.diagnostics.shared.FileMaker;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FileOutputTest {
  public static class StringFileout extends FileOutput {
    private final Map<String, byte[]> data = new HashMap<>();

    protected StringFileout(int timeoutSeconds, FileMaker fileMaker) {
      super(timeoutSeconds, fileMaker);
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    protected void writeFile(String fileName, byte[] data) throws IOException {
      this.data.put(fileName, data);
    }

    public byte[] getData(String fileName) {
      return this.data.get(fileName);
    }
  }

  private static String dataString;

  @BeforeAll
  static void setup() throws IOException {
    final String dir = "/tmp/dremio-test-12345";
    FileMaker maker =
        new FileMaker() {

          @Override
          public Path getNewDir() throws IOException {
            return Paths.get(dir);
          }
        };
    List<String> sources = new ArrayList<>();
    sources.add("mySource");
    StringFileout fileOutput = new StringFileout(60, maker);
    fileOutput.sourceOutput(sources, Optional.empty());
    fileOutput.close();
    final byte[] data = fileOutput.getData("create.sh");
    dataString = new String(data, StandardCharsets.UTF_8);
  }

  @Test
  void testMakeSourceDir() {
    assertThat(dataString).contains("\nmkdiriflocal /tmp/dremio-test-12345\n");
  }

  @Test
  void testSetPath() {
    assertThat(dataString)
        .contains(" \\\"config\\\": {\\\"path\\\": \\\"/tmp/dremio-test-12345\\\"},\n");
  }

  @Test
  void testSetAutopromotion() {
    assertThat(dataString).contains(" \\\"autoPromoteDatasets\\\": true\n");
  }

  @Test
  void testDoesNotContainCTASFormating() {
    assertThat(dataString).doesNotContain("defaultCtasFormat");
  }

  @Nested
  static class WithNonDefaultCTASFormat {
    @Test
    void testCTASFormatWithParquet() throws IOException {
      final String dir = "/tmp/dremio-test-12345";
      FileMaker maker =
          new FileMaker() {

            @Override
            public Path getNewDir() throws IOException {
              return Paths.get(dir);
            }
          };
      List<String> sources = new ArrayList<>();
      sources.add("mySource");
      StringFileout fileOutput = new StringFileout(60, maker);
      fileOutput.sourceOutput(sources, Optional.of("PARQUET"));
      fileOutput.close();
      final byte[] data = fileOutput.getData("create.sh");
      final String str = new String(data, StandardCharsets.UTF_8);
      assertThat(str).contains("\\\"defaultCtasFormat\\\":\\\"PARQUET\\\"");
    }
  }
}
