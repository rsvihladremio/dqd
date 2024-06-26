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
package com.dremio.support.diagnostics.profilejson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class HtmlProfileComparisonReportTest {

  @Test
  void testGetTitle() throws IOException {
    final ProfileJSONParser parser = new ProfileJSONParser();
    final ProfileJSON parsed1 = parser.parseFile(FileTestHelpers.getTestProfile1().stream());
    final ProfileJSON parsed2 = parser.parseFile(FileTestHelpers.getTestProfile2().stream());
    // the content doesn't actually matter, we can spin up an empty report
    Path path = Paths.get("");
    assertThat(
        new HtmlProfileComparisonReport(
            false,
            path.toString(),
            path.toString(),
            parsed1,
            parsed2,
            new ArrayList<>())
            .getTitle())
        .isEqualTo("Profile.json Analysis");
  }

  @Test
  void testNullProfile() throws IOException {
    final ProfileJSONParser parser = new ProfileJSONParser();
    final ProfileJSON profile1 = parser.parseFile(FileTestHelpers.getTestProfile1().stream());
    final ProfileJSON profile2 = parser.parseFile(FileTestHelpers.getTestProfile2().stream());

    final String file1 = FileTestHelpers.getTestProfile1().filePath().toString();
    final String file2 = FileTestHelpers.getTestProfile2().filePath().toString();
    assertThatThrownBy(
        () -> new HtmlProfileComparisonReport(
            false,
            file1,
            file2,
            null,
            profile2, new ArrayList<>()).getText())
        .hasMessageContaining("profile1 cannot be null");
    assertThatThrownBy(
        () -> new HtmlProfileComparisonReport(false, file1, file2, profile1, null, new ArrayList<>()).getText())
        .hasMessageContaining("profile2 cannot be null");
  }

  @Test
  public void testNullTextReport() throws IOException {
    final ProfileJSONParser parser = new ProfileJSONParser();
    ProfileJSON profile1 = parser.parseFile(FileTestHelpers.getTestProfile1().stream());
    ProfileJSON profile2 = parser.parseFile(FileTestHelpers.getTestProfile2().stream());
    final String file1 = FileTestHelpers.getTestProfile1().filePath().toString();
    final String file2 = FileTestHelpers.getTestProfile2().filePath().toString();
    assertThatThrownBy(
        () -> new HtmlProfileComparisonReport(false, file1, file2, profile1, profile2, null).getText())
        .hasMessageContaining("the console report cannot be null, this is a critical bug");
  }
}
