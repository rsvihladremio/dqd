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
package com.dremio.support.diagnostics.profilejson.singlefile;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.profilejson.ProfileJSONParser;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class SingleProfileJsonHtmlReportTest {

  @Test
  void getTextWhenEmptyProfile() {
    ProfileJSON profileJSON = new ProfileJSON();
    SingleProfileJsonHtmlReport report = new SingleProfileJsonHtmlReport(false, false, profileJSON);
    assertThat(report.getText()).contains("start time");
    assertThat(report.getText()).contains("end time");
  }

  /**
   * characterization test that locks in behavior and will fail if the report
   * changes
   *
   * @throws IOException when unable to access resource
   */
  @Test
  void getText() throws IOException {
    Locale.setDefault(Locale.US);
    ProfileJSONParser parser = new ProfileJSONParser();
    ProfileJSON profileJSON = parser.parseFile(FileTestHelpers.getTestProfile1().stream());
    SingleProfileJsonHtmlReport report = new SingleProfileJsonHtmlReport(true, false, profileJSON);
    String text = report.getText();
    assertThat(text).contains("<td>Thu, 13 Oct 2022 11:12:56 GMT</td>"); // query start
    assertThat(text).contains("<td>Thu, 13 Oct 2022 11:12:59 GMT</td>"); // query end
    assertThat(text).contains("<td>2.42 seconds</td>"); // total query time
    assertThat(text).contains("<td>COMPLETED</td>"); // query status
    assertThat(text).contains("<td>Low Cost User Queries</td>"); // queue
    assertThat(text).contains("<td>8.00 gb</td>"); // coordinator max direct memory
    assertThat(text).contains("<td>8</td>"); // coordinator available cores
    assertThat(text).contains("<td>laptop-jkcfofo7.home</td>"); // coordinator host name
    assertThat(text)
        .contains(
            "<td>21.6.0-202209301921120677-ad35777b</td>"); // dremio version (client and server are
    assertThat(text).contains("SELECT \"SF weather 2018-2019.csv\".\"DATE\" AS"); // part of query
    assertThat(text).contains("\"DATE\", ELEVATION, MAX(TAVG) AS Maximum_TAVG,");
    assertThat(text).contains("MIN(TMIN) AS Minimum_TMIN, MAX(TMAX) AS");
    // phase analysis
    assertThat(text)
        .contains(
            "x: [902,], text: [\"00-00-XX - run 898 millis, sleep 0 millis, blocked {"
                + " total 1 milli, upstream 0 millis, downstream 1 milli, shared"
                + " 0 millis }\",],  orientation: 'h', mode: 'markers', xaxis: 'x', yaxis:"
                + " 'y', type: 'scatter', name:'phase thread process time',};");
  }

  @Test
  void getTitle() {
    ProfileJSON profileJSON = new ProfileJSON();
    SingleProfileJsonHtmlReport report = new SingleProfileJsonHtmlReport(false, false, profileJSON);
    assertThat(report.getTitle()).isEqualTo("Profile.json Analysis");
  }
}
