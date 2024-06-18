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
package com.dremio.support.diagnostics;

import com.dremio.support.diagnostics.shared.PathAndStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class FileTestHelpers {

  public static byte[] ReadAllBytes(InputStream stream) throws IOException {
    int bufferSize = 4096;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    int read;
    byte[] buff = new byte[bufferSize];
    while ((read = stream.read(buff, 0, bufferSize)) != -1) {
      bos.write(buff, 0, read);
    }
    return bos.toByteArray();
  }

  private FileTestHelpers() {}

  public static PathAndStream getStressSimpleYaml() {
    return getResource("com/dremio/support/diagnostics/stress-simple.yaml");
  }

  public static PathAndStream getStressOrderedYaml() {
    return getResource("com/dremio/support/diagnostics/stress-grouped.yaml");
  }

  public static PathAndStream getStressYaml() {
    return getResource("com/dremio/support/diagnostics/stress.yaml");
  }

  public static PathAndStream getTestProfile1() {
    return getResource("com/dremio/support/diagnostics/profilejson/profile1.json");
  }

  public static PathAndStream getTestProfile2() {
    return getResource("com/dremio/support/diagnostics/profilejson/profile2.json");
  }

  public static PathAndStream getTestEmptyProfile() {
    return getResource("com/dremio/support/diagnostics/profilejson/empty.json");
  }

  public static PathAndStream getTestProfileWithEmptyClientInfo() {
    return getResource(
        "com/dremio/support/diagnostics/profilejson/profileWithEmptyClientInfo.json");
  }

  public static PathAndStream getTestProfileWithNoClientInfo() {
    return getResource("com/dremio/support/diagnostics/profilejson/profileWithNoClientInfo.json");
  }

  public static PathAndStream getTestQueriesJsonInGunzip() {
    return getResource("queries.json.gz");
  }

  public static String getTestQueriesJsonText() throws IOException {
    final PathAndStream pathAndStream = getResource("queries.json");
    final InputStream inputStream = pathAndStream.stream();
    return readAll(inputStream);
  }

  public static String readAll(final InputStream inputStream) throws IOException {
    final StringBuilder builder = new StringBuilder();
    final int bufferSize = 4096;
    final byte[] buffer = new byte[bufferSize];
    while ((inputStream.read(buffer) != -1)) {
      builder.append(new String(buffer, StandardCharsets.UTF_8));
    }
    System.out.println(builder.toString());
    return builder.toString();
  }

  private static PathAndStream getResource(String url) {
    URI uri;
    try {
      uri = ClassLoader.getSystemResource(url).toURI();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return new PathAndStream(Paths.get(uri), ClassLoader.getSystemResourceAsStream(url));
  }

  public static PathAndStream getTestHtml() {
    return getResource("com/dremio/support/diagnostics/test.html");
  }
}
