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

import com.dremio.support.diagnostics.shared.JsonTextProfileProvider;
import com.dremio.support.diagnostics.shared.PathAndStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class JsonTextProfileProviderTest {

  @Test
  void testParseProfile() throws IOException, URISyntaxException {
    URL resource =
        this.getClass().getResource("/com/dremio/support/diagnostics/profilejson/profile1.json");
    if (resource == null) {
      throw new FileNotFoundException("profile1.json not found for test");
    }
    try (FileInputStream fs = new FileInputStream(resource.getFile())) {
      JsonTextProfileProvider provider =
          new JsonTextProfileProvider(new PathAndStream(Paths.get(resource.toURI()), fs));
      assertThat(provider.getProfile()).isNotNull();
    }
  }
}
