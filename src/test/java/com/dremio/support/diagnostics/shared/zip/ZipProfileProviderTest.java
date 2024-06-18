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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.dremio.support.diagnostics.profilejson.ProfileJSONParser;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ZipProfileProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ZipProfileProviderTest {

  private static ProfileJSON profile;

  @BeforeAll
  static void initAll() throws IOException {
    URL resource = ClassLoader.getSystemClassLoader().getResource("testprofile.zip");
    if (resource == null) {
      throw new RuntimeException("test setup not correct as testprofile.zip is not present");
    }
    File zipFile = new File(resource.getFile());
    ProfileJSONParser parser = new ProfileJSONParser();
    UnzipperImpl unzipper = new UnzipperImpl();
    try (FileInputStream fs = new FileInputStream(zipFile)) {
      PathAndStream stream = new PathAndStream(zipFile.toPath(), fs);
      ZipProfileProvider provider = new ZipProfileProvider(parser, unzipper, stream);
      profile = provider.getProfile();
    }
  }

  @Test
  void testThatProfileIsNotNull() {
    assertThat(profile).isNotNull();
  }

  @Test
  void testThatDremioVersionIsParsed() {
    assertThat(profile.getDremioVersion()).isEqualTo("21.6.2-202210141639540835-ecf959e4");
  }
}

class ZipProfileWithMultipleJSONValues {
  private static ProfileJSON profile;

  @BeforeAll
  static void initAll() throws IOException {
    URL resource = ClassLoader.getSystemClassLoader().getResource("bunchofjson.zip");
    if (resource == null) {
      throw new RuntimeException("test setup not correct as bunchfojson.zip is not present");
    }
    File zipFile = new File(resource.getFile());
    ProfileJSONParser parser = new ProfileJSONParser();
    UnzipperImpl unzipper = new UnzipperImpl();
    try (FileInputStream fs = new FileInputStream(zipFile)) {
      PathAndStream stream = new PathAndStream(zipFile.toPath(), fs);
      ZipProfileProvider provider = new ZipProfileProvider(parser, unzipper, stream);
      profile = provider.getProfile();
    }
  }

  @Test
  void testProfileIsNotNull() {
    assertThat(profile).isNotNull();
  }

  @Test
  void testWeParsedTheProfileJson() throws IOException {
    // verifies we found the right json
    assertThat(profile.getForeman()).isNotNull();
  }
}
