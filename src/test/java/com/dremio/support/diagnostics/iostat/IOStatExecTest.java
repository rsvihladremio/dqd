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
package com.dremio.support.diagnostics.iostat;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class IOStatExecTest {

  @Test
  public void intergrationTest() throws URISyntaxException, IOException {
    final URL url = this.getClass().getResource("/iostat.txt");
    final Path file = Paths.get(url.toURI());
    try (final ByteArrayOutputStream boas = new ByteArrayOutputStream()) {
      try (final InputStream is = Files.newInputStream(file)) {
        IOStatExec.exec(is, boas);
        final String output = boas.toString("UTF-8");
        String expectedCPUBottleNeck =
            """
</tr>
<tr><td>%time over 50% user+system+steal+nice cpu usage (for systems with 2 threads per core)</td>
<td>13.00%</td>
</tr>
""";
        assertTrue(output.contains(expectedCPUBottleNeck), "cpu bottleneck is missing");
        assertTrue(output.contains("11.17"), "max iowait is missing");
        assertTrue(output.contains("20.35"), "max 'average queue size' is missing");
      }
    }
  }
}
