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

import com.dremio.support.diagnostics.shared.Report;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import com.github.freva.asciitable.OverflowBehaviour;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** generates a string based report of the differences between two profiles */
public class ProfileJsonComparisonTextReport implements Report {
  private final List<Difference> differences;
  private final String profile1Name;
  private final String profile2Name;

  /**
   * create a console based report of the comparison between two profiles and their differences
   *
   * @param differences list of differences between two profiles
   * @param profile1Name the first profile file under comparison
   * @param profile2Name the second profile under comparison
   */
  public ProfileJsonComparisonTextReport(
      final List<Difference> differences, final Path profile1Name, final Path profile2Name) {
    if (profile1Name == null) {
      throw new IllegalArgumentException("profile 1 has no name, this is a critical bug");
    }
    if (profile2Name == null) {
      throw new IllegalArgumentException("profile 2 has no name, this is a critical bug");
    }
    if (differences == null) {
      throw new IllegalArgumentException("differences cannot be null, this is a critical bug");
    }
    this.differences = new ArrayList<>(differences);
    this.profile1Name = profile1Name.toString();
    this.profile2Name = profile2Name.toString();
  }

  /**
   * generate string report of the comparison of profiles. Generates a pleasant to read ascii table
   *
   * @return ascii table summary of the profile json comparison analysis
   * @throws IOException either from encoding error of the data or inability to write the string
   */
  @Override
  public String getText() throws IOException {
    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
      // choosing some basic widths 20, 50, 50 for the three columns
      // overflow for the text is handled by newline, this is so we can see
      // all the data and have none of it trimmed, but the fixed width means the
      // report will fit on most screens
      // header is center aligned
      // test is left aligned as we need a lot of space
      AsciiTable.builder()
          .data(
              differences,
              Arrays.asList(
                  new Column()
                      .header("name")
                      .headerAlign(HorizontalAlign.CENTER)
                      .maxWidth(20, OverflowBehaviour.NEWLINE)
                      .dataAlign(HorizontalAlign.LEFT)
                      .with(Difference::getName),
                  new Column()
                      .header(profile1Name)
                      .headerAlign(HorizontalAlign.CENTER)
                      .maxWidth(50, OverflowBehaviour.NEWLINE)
                      .dataAlign(HorizontalAlign.LEFT)
                      .with(Difference::getProfile1Value),
                  new Column()
                      .header(profile2Name)
                      .headerAlign(HorizontalAlign.CENTER)
                      .maxWidth(50, OverflowBehaviour.NEWLINE)
                      .dataAlign(HorizontalAlign.LEFT)
                      .with(Difference::getProfile2Value),
                  new Column()
                      .header("advice")
                      .headerAlign(HorizontalAlign.CENTER)
                      .dataAlign(HorizontalAlign.LEFT)
                      .with(Difference::getAdvice)))
          .writeTo(stream);
      // assuming UTF8 for conversion, there is nothing intelligent about this, just a guess.
      return stream.toString("UTF8");
    }
  }

  /**
   * read only string so consumers can know the title of the report
   *
   * @return title of the report
   */
  @Override
  public String getTitle() {
    return "Profile Json Comparison Report";
  }
}
