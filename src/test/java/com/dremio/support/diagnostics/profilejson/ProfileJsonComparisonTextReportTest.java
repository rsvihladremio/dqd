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

import static com.dremio.support.diagnostics.FileTestHelpers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProfileJsonComparisonTextReportTest {

  @Test
  void testGetText() throws IOException {
    List<Difference> differences = new ArrayList<>();

    Difference diff1 = new Difference();
    diff1.setName("metric1");
    diff1.setProfile1Value("0.56");
    diff1.setProfile2Value("0.99");
    diff1.setAdvice("stop doing that");

    Difference diff2 = new Difference();
    diff2.setName("metric2");
    diff2.setProfile1Value("0");
    diff2.setProfile2Value("1000");
    diff2.setAdvice("do more");

    differences.add(diff1);
    differences.add(diff2);
    String profile1 = "profile1.json";
    String profile2 = "profile2.json";
    String text =
        new ProfileJsonComparisonTextReport(differences, Paths.get(profile1), Paths.get(profile2))
            .getText();
    assertThat(text).contains(" do more ");
    assertThat(text).contains(" stop doing that ");
    assertThat(text).contains(" 0.99 ");
    assertThat(text).contains(" 0.56 ");
    assertThat(text).contains(" 0 ");
    assertThat(text).contains(" 1000 ");
    assertThat(text).contains(" metric1 ");
    assertThat(text).contains(" metric2 ");
    assertThat(text).contains(" profile1.json ");
    assertThat(text).contains(" profile2.json ");
  }

  @Test
  void testGetTitle() {
    // the content doesn't actually matter, we can spin up an empty report
    Path profile1Name = Paths.get("");
    assertThat(
            new ProfileJsonComparisonTextReport(new ArrayList<>(), profile1Name, profile1Name)
                .getTitle())
        .isEqualTo("Profile Json Comparison Report");
  }

  @Test
  void testNullProfile() {
    assertThatThrownBy(
            () ->
                new ProfileJsonComparisonTextReport(new ArrayList<>(), null, Paths.get("abc"))
                    .getText())
        .hasMessageContaining("profile 1 has no name, this is a critical bug");
    assertThatThrownBy(
            () ->
                new ProfileJsonComparisonTextReport(new ArrayList<>(), Paths.get("abc"), null)
                    .getText())
        .hasMessageContaining("profile 2 has no name, this is a critical bug");
  }

  @Test
  public void testNullArrayList() {
    assertThatThrownBy(
            () ->
                new ProfileJsonComparisonTextReport(
                        null, getTestProfile1().filePath(), getTestProfile2().filePath())
                    .getTitle())
        .hasMessageContaining("differences cannot be null, this is a critical bug");
  }
}
