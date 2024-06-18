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
package com.dremio.support.diagnostics.simple;

import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemErrAndOut;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.dremio.support.diagnostics.App;
import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.profilejson.CoreOperatorType;
import com.dremio.support.diagnostics.repro.ArgSetup;
import java.io.IOException;
import java.util.Locale;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class ProfileJSONSimplifiedTest {

  @Test
  void testZeroDurationNanosToSeconds() {
    var nearZeroOffset = Offset.offset(0.0000001);
    var mostlyEmpty =
        new ProfileJSONSimplified.OperatorRow(
            "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, CoreOperatorType.SCREEN, "");
    assertThat(mostlyEmpty.processNanos()).isEqualTo(0);
    assertThat(mostlyEmpty.processSeconds()).isEqualTo(0.0, nearZeroOffset);
    assertThat(mostlyEmpty.waitNanos()).isEqualTo(0);
    assertThat(mostlyEmpty.waitSeconds()).isEqualTo(0.0, nearZeroOffset);
    assertThat(mostlyEmpty.setupNanos()).isEqualTo(0);
    assertThat(mostlyEmpty.setupSeconds()).isEqualTo(0.0, nearZeroOffset);
    assertThat(mostlyEmpty.totalDurationNanos()).isEqualTo(0);
    assertThat(mostlyEmpty.totalDurationSeconds()).isEqualTo(0.0, nearZeroOffset);
  }

  @Nested
  class SummarizeTest {

    @Test
    public void TestSingleProfile() throws IOException {
      Locale.setDefault(new Locale("en", "US"));
      var summarize = new ProfileJSONSimplified.Summarize();
      var singleProfile = FileTestHelpers.getTestProfile1();
      var profile = ArgSetup.getProfileProvider(singleProfile);
      var summary = summarize.singleProfile(profile.getProfile());
      assertThat(summary.dremioVersion()).isEqualTo("21.6.0-202209301921120677-ad35777b");
      assertThat(summary.user()).isEqualTo("rssvihla");
      assertThat(summary.queryPhase()).isEqualTo("COMPLETED");
      assertThat(summary.endEpochMillis()).isEqualTo(1665659579063L);
      assertThat(summary.startEpochMillis()).isEqualTo(1665659576641L);
      assertThat(summary.operatorRows().size()).isEqualTo(9);
      final ProfileJSONSimplified.OperatorRow[] rows =
          summary.operatorRows().toArray(new ProfileJSONSimplified.OperatorRow[0]);
      var first = rows[0];
      assertThat(first.name()).isEqualTo("TEXT_SUB_SCAN 00-00-08");
      assertThat(first.batches()).isEqualTo(2);
      assertThat(first.records()).isEqualTo(485);
      assertThat(first.peakMemoryAllocatedBytes()).isEqualTo(1180416L);
      assertThat(first.setupNanos()).isEqualTo(1019591L);
      assertThat(first.setupSeconds()).isEqualTo(0.001, Offset.offset(0.001));
      assertThat(first.waitSeconds()).isEqualTo(0.877, Offset.offset(0.001));
      assertThat(first.processSeconds()).isEqualTo(0.000583, Offset.offset(0.001));
      assertThat(first.totalDurationSeconds()).isEqualTo(0.878, Offset.offset(0.001));
      assertThat(first.coreOperatorType()).isEqualTo(CoreOperatorType.TEXT_SUB_SCAN);
      assertThat(first.hostName()).isEqualTo("laptop-jkcfofo7.home");

      var last = rows[8];
      assertThat(last.name()).isEqualTo("SCREEN 00-00-00");
      assertThat(last.batches()).isEqualTo(1);
      assertThat(last.records()).isEqualTo(1);
      assertThat(last.setupNanos()).isEqualTo(2680L);
      assertThat(last.setupSeconds()).isEqualTo(0.000, Offset.offset(0.001));
      assertThat(last.waitSeconds()).isEqualTo(0.000, Offset.offset(0.001));
      assertThat(last.processSeconds()).isEqualTo(0.000128, Offset.offset(0.0001));
      assertThat(last.totalDurationSeconds()).isEqualTo(0.00025, Offset.offset(0.0001));
      assertThat(last.coreOperatorType()).isEqualTo(CoreOperatorType.SCREEN);

      assertThat(last.hostName()).isEqualTo("laptop-jkcfofo7.home");
      assertThat(last.peakMemoryAllocatedBytes()).isEqualTo(1000000L);
      assertThat(summary.findings().size()).isEqualTo(1);
      assertThat(summary.totalPhases()).isEqualTo(1);

      var finding = summary.findings().stream().findFirst();
      assertThat(finding.get())
          .isEqualTo(
              "97.36 % of the query time is taken up by the following phases [(METADATA_RETRIEVAL"
                  + " 60.12%), (RUNNING 37.24%)]");
    }

    @Test
    public void TestSingleProfileIsNull() {
      var summarize = new ProfileJSONSimplified.Summarize();
      var summary = summarize.singleProfile(null);
      assertThat(summary.dremioVersion()).isEqualTo("unknown");
      assertThat(summary.user()).isEqualTo("unknown user");
      assertThat(summary.queryPhase()).isEqualTo("UNKNOWN PHASE");
      assertThat(summary.endEpochMillis()).isEqualTo(0);
      assertThat(summary.startEpochMillis()).isEqualTo(0);
      assertThat(summary.operatorRows().size()).isEqualTo(0);
      assertThat(summary.findings().size()).isEqualTo(0);
      assertThat(summary.totalPhases()).isEqualTo(0);
    }

    @Test
    public void testCompareProfilesAreNull() {
      var summarize = new ProfileJSONSimplified.Summarize();
      var summaryCompare = summarize.compareProfiles(null, null);
      var summary1 = summaryCompare.summary1();
      assertThat(summary1.dremioVersion()).isEqualTo("unknown");
      assertThat(summary1.user()).isEqualTo("unknown user");
      assertThat(summary1.queryPhase()).isEqualTo("UNKNOWN PHASE");
      assertThat(summary1.endEpochMillis()).isEqualTo(0);
      assertThat(summary1.startEpochMillis()).isEqualTo(0);
      assertThat(summary1.operatorRows().size()).isEqualTo(0);
      assertThat(summary1.findings().size()).isEqualTo(0);
      assertThat(summary1.totalPhases()).isEqualTo(0);
      var summary2 = summaryCompare.summary1();
      assertThat(summary2.dremioVersion()).isEqualTo("unknown");
      assertThat(summary2.user()).isEqualTo("unknown user");
      assertThat(summary2.queryPhase()).isEqualTo("UNKNOWN PHASE");
      assertThat(summary2.endEpochMillis()).isEqualTo(0);
      assertThat(summary2.startEpochMillis()).isEqualTo(0);
      assertThat(summary2.operatorRows().size()).isEqualTo(0);
      assertThat(summary2.findings().size()).isEqualTo(0);
      assertThat(summary2.totalPhases()).isEqualTo(0);
    }
  }

  @Nested
  class IntegrationTest {

    @Test
    void testSingleProfileOutputHasOperatorTable() throws Exception {
      Locale.setDefault(Locale.US);
      String text =
          tapSystemErrAndOut(
              () -> {
                CommandLine cmd = new CommandLine(new App());
                cmd.execute(
                    "summarize-profile-json",
                    FileTestHelpers.getTestProfile1().filePath().toAbsolutePath().toString());
              });

      assertThat(text)
          .contains(
              """
                  <thead>
                  <th>Name</th>
                  <th>Process</th>
                  <th>Wait</th>
                  <th>Setup</th>
                  <th>Total</th>
                  <th>Size Processed</th>
                  <th>Batches</th>
                  <th>Records</th>
                  <th>Records/Sec</th>
                  <th>Peak RAM Allocated</th>
                  <th>node</th>
                  </thead>
                  <tbody>
                  <tr><td>TEXT_SUB_SCAN 00-00-08</td>
                  <td data-sort="583322">0 millis</td>
                  <td data-sort="877221339">877 millis</td>
                  <td data-sort="1019591">1 milli</td>
                  <td data-sort="878824252">878 millis</td>
                  <td data-sort="18753">18.31 kb</td>
                  <td data-sort="2">2</td>
                  <td data-sort="485">485</td>
                  <td data-sort="551.87">551.87</td>
                  <td data-sort="1180416">1.13 mb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>HASH_AGGREGATE 00-00-06</td>
                  <td data-sort="3240568">3 millis</td>
                  <td data-sort="0">0 millis</td>
                  <td data-sort="2036322">2 millis</td>
                  <td data-sort="5276890">5 millis</td>
                  <td data-sort="18753">18.31 kb</td>
                  <td data-sort="1">1</td>
                  <td data-sort="485">485</td>
                  <td data-sort="91910.2">91,910.2</td>
                  <td data-sort="10027008">9.56 mb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>ARROW_WRITER 00-00-03</td>
                  <td data-sort="1046003">1 milli</td>
                  <td data-sort="881656">0 millis</td>
                  <td data-sort="260439">0 millis</td>
                  <td data-sort="2188098">2 millis</td>
                  <td data-sort="18903">18.46 kb</td>
                  <td data-sort="8">8</td>
                  <td data-sort="485">485</td>
                  <td data-sort="221653.69">221,653.69</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>PROJECT 00-00-07</td>
                  <td data-sort="66321">0 millis</td>
                  <td data-sort="0">0 millis</td>
                  <td data-sort="779257">0 millis</td>
                  <td data-sort="845578">0 millis</td>
                  <td data-sort="18753">18.31 kb</td>
                  <td data-sort="1">1</td>
                  <td data-sort="485">485</td>
                  <td data-sort="573572.16">573,572.16</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>PROJECT 00-00-05</td>
                  <td data-sort="112761">0 millis</td>
                  <td data-sort="0">0 millis</td>
                  <td data-sort="705821">0 millis</td>
                  <td data-sort="818582">0 millis</td>
                  <td data-sort="18903">18.46 kb</td>
                  <td data-sort="8">8</td>
                  <td data-sort="485">485</td>
                  <td data-sort="592487.99">592,487.99</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>WRITER_COMMITTER 00-00-02</td>
                  <td data-sort="45104">0 millis</td>
                  <td data-sort="0">0 millis</td>
                  <td data-sort="532317">0 millis</td>
                  <td data-sort="577421">0 millis</td>
                  <td data-sort="964">964 bytes</td>
                  <td data-sort="1">1</td>
                  <td data-sort="1">1</td>
                  <td data-sort="1731.84">1,731.84</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>PROJECT 00-00-01</td>
                  <td data-sort="42634">0 millis</td>
                  <td data-sort="0">0 millis</td>
                  <td data-sort="403127">0 millis</td>
                  <td data-sort="445761">0 millis</td>
                  <td data-sort="964">964 bytes</td>
                  <td data-sort="1">1</td>
                  <td data-sort="1">1</td>
                  <td data-sort="2243.35">2,243.35</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>PROJECT 00-00-04</td>
                  <td data-sort="64070">0 millis</td>
                  <td data-sort="0">0 millis</td>
                  <td data-sort="368539">0 millis</td>
                  <td data-sort="432609">0 millis</td>
                  <td data-sort="18903">18.46 kb</td>
                  <td data-sort="8">8</td>
                  <td data-sort="485">485</td>
                  <td data-sort="1121104.74">1,121,104.74</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  <tr><td>SCREEN 00-00-00</td>
                  <td data-sort="123848">0 millis</td>
                  <td data-sort="130276">0 millis</td>
                  <td data-sort="2680">0 millis</td>
                  <td data-sort="256804">0 millis</td>
                  <td data-sort="964">964 bytes</td>
                  <td data-sort="1">1</td>
                  <td data-sort="1">1</td>
                  <td data-sort="3894.02">3,894.02</td>
                  <td data-sort="1000000">976.56 kb</td>
                  <td>laptop-jkcfofo7.home</td>
                  </tr>
                  </tbody>
                  </table>""");
    }
  }
}
