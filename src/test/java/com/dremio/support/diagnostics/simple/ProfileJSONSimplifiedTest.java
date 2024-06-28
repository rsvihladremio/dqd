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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.support.diagnostics.App;
import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.profilejson.CoreOperatorType;
import com.dremio.support.diagnostics.repro.ArgSetup;
import java.io.IOException;
import java.util.Locale;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class ProfileJSONSimplifiedTest {

  @Test
  void testZeroDurationNanosToSeconds() {
    var nearZeroOffset = 0.0000001;
    var mostlyEmpty =
        new ProfileJSONSimplified.OperatorRow(
            "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, CoreOperatorType.SCREEN, "");
    assertEquals(mostlyEmpty.processNanos(), 0);
    assertEquals(mostlyEmpty.processSeconds(), 0.0, nearZeroOffset);
    assertEquals(mostlyEmpty.waitNanos(), 0);
    assertEquals(mostlyEmpty.waitSeconds(), 0.0, nearZeroOffset);
    assertEquals(mostlyEmpty.setupNanos(), 0);
    assertEquals(mostlyEmpty.setupSeconds(), 0.0, nearZeroOffset);
    assertEquals(mostlyEmpty.totalDurationNanos(), 0);
    assertEquals(mostlyEmpty.totalDurationSeconds(), 0.0, nearZeroOffset);
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
      assertEquals(summary.dremioVersion(), "21.6.0-202209301921120677-ad35777b");
      assertEquals(summary.user(), "rssvihla");
      assertEquals(summary.queryPhase(), "COMPLETED");
      assertEquals(summary.endEpochMillis(), 1665659579063L);
      assertEquals(summary.startEpochMillis(), 1665659576641L);
      assertEquals(summary.operatorRows().size(), 9);
      final ProfileJSONSimplified.OperatorRow[] rows =
          summary.operatorRows().toArray(new ProfileJSONSimplified.OperatorRow[0]);
      var first = rows[0];
      assertEquals(first.name(), "TEXT_SUB_SCAN 00-00-08");
      assertEquals(first.batches(), 2);
      assertEquals(first.records(), 485);
      assertEquals(first.peakMemoryAllocatedBytes(), 1180416L);
      assertEquals(first.setupNanos(), 1019591L);
      assertEquals(first.setupSeconds(), 0.001, 0.001);
      assertEquals(first.waitSeconds(), 0.877, 0.001);
      assertEquals(first.processSeconds(), 0.000583, 0.001);
      assertEquals(first.totalDurationSeconds(), 0.878, 0.001);
      assertEquals(first.coreOperatorType(), CoreOperatorType.TEXT_SUB_SCAN);
      assertEquals(first.hostName(), "laptop-jkcfofo7.home");

      var last = rows[8];
      assertEquals(last.name(), "SCREEN 00-00-00");
      assertEquals(last.batches(), 1);
      assertEquals(last.records(), 1);
      assertEquals(last.setupNanos(), 2680L);
      assertEquals(last.setupSeconds(), 0.000, 0.001);
      assertEquals(last.waitSeconds(), 0.000, 0.001);
      assertEquals(last.processSeconds(), 0.000128, 0.0001);
      assertEquals(last.totalDurationSeconds(), 0.00025, 0.0001);
      assertEquals(last.coreOperatorType(), CoreOperatorType.SCREEN);

      assertEquals(last.hostName(), "laptop-jkcfofo7.home");
      assertEquals(last.peakMemoryAllocatedBytes(), 1000000L);
      assertEquals(summary.findings().size(), 1);
      assertEquals(summary.totalPhases(), 1);

      var finding = summary.findings().stream().findFirst();
      assertEquals(
          finding.get(),
          "97.36 % of the query time is taken up by the following phases [(METADATA_RETRIEVAL"
              + " 60.12%), (RUNNING 37.24%)]");
    }

    @Test
    public void TestSingleProfileIsNull() {
      var summarize = new ProfileJSONSimplified.Summarize();
      var summary = summarize.singleProfile(null);
      assertEquals(summary.dremioVersion(), "unknown");
      assertEquals(summary.user(), "unknown user");
      assertEquals(summary.queryPhase(), "UNKNOWN PHASE");
      assertEquals(summary.endEpochMillis(), 0);
      assertEquals(summary.startEpochMillis(), 0);
      assertEquals(summary.operatorRows().size(), 0);
      assertEquals(summary.findings().size(), 0);
      assertEquals(summary.totalPhases(), 0);
    }

    @Test
    public void testCompareProfilesAreNull() {
      var summarize = new ProfileJSONSimplified.Summarize();
      var summaryCompare = summarize.compareProfiles(null, null);
      var summary1 = summaryCompare.summary1();
      assertEquals(summary1.dremioVersion(), "unknown");
      assertEquals(summary1.user(), "unknown user");
      assertEquals(summary1.queryPhase(), "UNKNOWN PHASE");
      assertEquals(summary1.endEpochMillis(), 0);
      assertEquals(summary1.startEpochMillis(), 0);
      assertEquals(summary1.operatorRows().size(), 0);
      assertEquals(summary1.findings().size(), 0);
      assertEquals(summary1.totalPhases(), 0);
      var summary2 = summaryCompare.summary1();
      assertEquals(summary2.dremioVersion(), "unknown");
      assertEquals(summary2.user(), "unknown user");
      assertEquals(summary2.queryPhase(), "UNKNOWN PHASE");
      assertEquals(summary2.endEpochMillis(), 0);
      assertEquals(summary2.startEpochMillis(), 0);
      assertEquals(summary2.operatorRows().size(), 0);
      assertEquals(summary2.findings().size(), 0);
      assertEquals(summary2.totalPhases(), 0);
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
      System.out.println(text);
      assertTrue(
          text.contains(
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
              <tbody>"""));
      assertTrue(
          text.contains(
              """
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
              """));
      assertTrue(
          text.contains(
              """
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
              </table>"""));
    }
  }
}
