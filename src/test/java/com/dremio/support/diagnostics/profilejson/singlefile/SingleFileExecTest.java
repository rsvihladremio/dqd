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

import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemOut;
import static org.assertj.core.api.Assertions.*;

import com.dremio.support.diagnostics.profilejson.*;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.*;
import java.io.File;
import java.nio.file.Files;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class SingleFileExecTest {

  @BeforeEach
  public void setup() {
    Locale.setDefault(new Locale("en", "USA"));

    MockitoAnnotations.openMocks(this);
  }

  @Mock private ProfileProvider parser;

  @Test
  void testSingleFileExec() throws Exception {
    final File file = Files.createTempFile("test", "json").toFile();
    file.deleteOnExit();
    final ProfileJSON profileJson = new ProfileJSON();
    final Id id = new Id();
    id.setPart1(123456789);
    id.setPart2(5555);
    profileJson.setId(id);
    profileJson.setCommandPoolWaitMillis(60000);
    profileJson.setStart(1000L);
    profileJson.setEnd(2000L);
    profileJson.setDremioVersion("19.1.1");
    final ClientInfo clientInfo = new ClientInfo();
    clientInfo.setVersion("22.1.1");
    clientInfo.setName("Bob's JDBC");
    final Foreman foreman = new Foreman();
    foreman.setAddress("mycoordinator");
    foreman.setAvailableCores(60);
    foreman.setMaxDirectMemory((double) 1024 * 1024 * 1024);
    profileJson.setForeman(foreman);
    profileJson.setClientInfo(clientInfo);

    final FragmentProfile majorFragment1 = new FragmentProfile();
    majorFragment1.setMajorFragmentId(0);
    final MinorFragmentProfile minorFragment1 = new MinorFragmentProfile();
    // most blocked
    minorFragment1.setMinorFragmentId(0);
    minorFragment1.setStartTime(150L);
    minorFragment1.setEndTime(260L);
    minorFragment1.setSetupDuration(20L);
    minorFragment1.setBlockedDuration(100L);
    minorFragment1.setBlockedOnDownstreamDuration(20L);
    minorFragment1.setBlockedOnUpstreamDuration(45L);
    minorFragment1.setBlockedOnSharedResourceDuration(35L);
    final MinorFragmentProfile minorFragment2 = new MinorFragmentProfile();
    // longest running
    minorFragment2.setMinorFragmentId(1);
    minorFragment2.setStartTime(100L);
    minorFragment2.setEndTime(350L);
    minorFragment2.setRunDuration(225L);
    minorFragment2.setSetupDuration(5L);
    minorFragment2.setBlockedDuration(20L);
    majorFragment1.setMinorFragmentProfile(Arrays.asList(minorFragment1, minorFragment2));

    final FragmentProfile majorFragment2 = new FragmentProfile();
    majorFragment2.setMajorFragmentId(1);
    final FragmentProfile majorFragment3 = new FragmentProfile();
    majorFragment3.setMajorFragmentId(2);
    final FragmentProfile majorFragment4 = new FragmentProfile();
    majorFragment4.setMajorFragmentId(3);
    profileJson.setFragmentProfile(
        Arrays.asList(majorFragment1, majorFragment2, majorFragment3, majorFragment4));
    profileJson.setQuery("SELECT * FROM MYTABLE");
    profileJson.setState(QueryState.FAILED.ordinal());
    Mockito.when(parser.getProfile()).thenReturn(profileJson);
    final SingleFileExec exec = new SingleFileExec(parser);

    final String text = tapSystemOut(() -> exec.run(false));
    assertThat(text).contains(" 1 minute"); // command pool wait time
    assertThat(text).contains(" 1 second"); // query duration
    assertThat(text).contains(" 1024.00 mb"); // foreman direct memory
    assertThat(text).contains("mycoordinator"); // foreman host
    assertThat(text).contains(" 19.1.1"); // dremio version
    assertThat(text).contains("FAILED"); // query state
    assertThat(text).contains("SELECT * FROM MYTABLE"); // query text
    assertThat(text).contains(" 20 millis"); // blocked downstream time
    assertThat(text).contains(" 45 millis"); // blocked uptream time
    assertThat(text).contains(" 35 millis"); // blocked shared resource time
  }

  @Test
  void testFindMostExpensiveOperatorInPlan() throws Exception {
    final ProfileJSON profileJson = new ProfileJSON();
    final List<FragmentProfile> fragmentProfile = new ArrayList<>();
    final FragmentProfile fragment = new FragmentProfile();
    final List<MinorFragmentProfile> minorFragmentProfile = new ArrayList<>();
    final MinorFragmentProfile minorFragment = new MinorFragmentProfile();
    final List<OperatorProfile> operatorProfile = new ArrayList<>();
    final OperatorProfile operator1 = new OperatorProfile();
    operator1.setOperatorType(CoreOperatorType.HIVE_SUB_SCAN.ordinal());
    operatorProfile.add(operator1);
    final OperatorProfile slowestOperator = new OperatorProfile();
    slowestOperator.setOperatorType(CoreOperatorType.AVRO_SUB_SCAN.ordinal());
    slowestOperator.setSetupNanos(1000000L);
    slowestOperator.setProcessNanos(5000000L);
    slowestOperator.setPeakLocalMemoryAllocated(1024 * 5);
    final InputProfile slowestInput = new InputProfile();
    slowestInput.setRecords(1000000);
    slowestInput.setBatches(10);
    slowestInput.setSize(10);
    slowestOperator.setInputProfile(Collections.singletonList(slowestInput));
    operatorProfile.add(slowestOperator);

    minorFragment.setOperatorProfile(operatorProfile);
    minorFragment.setBlockedDuration(100L);
    minorFragment.setRunDuration(2000L);
    minorFragmentProfile.add(minorFragment);
    final MinorFragmentProfile mosttBlockedFragment = new MinorFragmentProfile();
    mosttBlockedFragment.setBlockedDuration(200L);
    mosttBlockedFragment.setRunDuration(200L);
    mosttBlockedFragment.setSetupDuration(100L);
    final OperatorProfile slowestOperatorOfmostBlockedPhase = new OperatorProfile();
    slowestOperatorOfmostBlockedPhase.setOperatorType(CoreOperatorType.ARROW_SUB_SCAN.ordinal());
    slowestOperatorOfmostBlockedPhase.setSetupNanos(1000000000L);
    slowestOperatorOfmostBlockedPhase.setWaitNanos(100000000L);
    slowestOperatorOfmostBlockedPhase.setProcessNanos(100000000L);
    slowestOperatorOfmostBlockedPhase.setPeakLocalMemoryAllocated(1000L);
    final InputProfile input = new InputProfile();
    input.setBatches(5222);
    input.setRecords(900000);
    input.setSize(100000);
    slowestOperatorOfmostBlockedPhase.setInputProfile(Collections.singletonList(input));
    mosttBlockedFragment.setMinorFragmentId(1);
    mosttBlockedFragment.setOperatorProfile(
        Arrays.asList(slowestOperatorOfmostBlockedPhase, new OperatorProfile()));
    minorFragmentProfile.add(mosttBlockedFragment);
    fragment.setMinorFragmentProfile(minorFragmentProfile);
    fragmentProfile.add(fragment);
    profileJson.setFragmentProfile(fragmentProfile);
    Mockito.when(parser.getProfile()).thenReturn(profileJson);
    final String text = tapSystemOut(() -> new SingleFileExec(parser).run(false));
    assertThat(text)
        .contains(
            "most records scanned by operator\n"
                + "---------------------------\n"
                + "  1. records(1,000,000) batches(10) memory used(5.00 kb) run time(5"
                + " millis) p/sec(20,000,000,000.00) name(00-00-00 AVRO_SUB_SCAN)\n"
                + "\t|\n"
                + "\t-> condition: null\n"
                + "\n"
                + "  2. records(900,000) batches(5,222) memory used(1000 bytes) run time(100"
                + " millis) p/sec(900,000,000.00) name(00-01-00 ARROW_SUB_SCAN)\n"
                + "\t|\n"
                + "\t-> condition: null\n");
  }

  @Test
  void testMemoryUsageByPhase() throws Exception {
    final ProfileJSON profileJson = new ProfileJSON();
    final List<FragmentProfile> fragmentProfile = new ArrayList<>();
    final FragmentProfile phase = new FragmentProfile();
    final List<MinorFragmentProfile> minorFragmentProfile = new ArrayList<>();
    final MinorFragmentProfile firstThread = new MinorFragmentProfile();
    final List<OperatorProfile> operatorProfile = new ArrayList<>();
    final OperatorProfile mostMemory = new OperatorProfile();
    mostMemory.setOperatorType(CoreOperatorType.SCREEN.ordinal());
    mostMemory.setPeakLocalMemoryAllocated(1024 * 15);
    operatorProfile.add(mostMemory);
    firstThread.setOperatorProfile(operatorProfile);
    minorFragmentProfile.add(firstThread);
    final MinorFragmentProfile secondThread = new MinorFragmentProfile();
    final OperatorProfile leastMemUsedOperator = new OperatorProfile();
    leastMemUsedOperator.setOperatorType(CoreOperatorType.FILTER.ordinal());
    leastMemUsedOperator.setOperatorId(1L);
    leastMemUsedOperator.setPeakLocalMemoryAllocated(1024 * 10);
    secondThread.setMinorFragmentId(1);
    secondThread.setOperatorProfile(Collections.singletonList(leastMemUsedOperator));
    minorFragmentProfile.add(secondThread);
    phase.setMinorFragmentProfile(minorFragmentProfile);
    fragmentProfile.add(phase);
    profileJson.setFragmentProfile(fragmentProfile);
    Mockito.when(parser.getProfile()).thenReturn(profileJson);
    final String text = tapSystemOut(() -> new SingleFileExec(parser).run(false));
    assertThat(text)
        .contains(
            "memory usage by phase\n---------------------\n"
                + "phase: 00-00 SCREEN - usage: 15.00 kb\n"
                + "phase: 00-01 FILTER - usage: 10.00 kb\n");
  }

  @Test
  void testSingleFileExecIsEmpty() throws Exception {
    File file = null;
    try {
      file = File.createTempFile("test", ".json");
      final File file2 = file;
      Mockito.when(parser.getProfile()).thenReturn(null);
      Mockito.when(parser.getFilePath()).thenReturn(file2.toPath());
      assertThatThrownBy(() -> new SingleFileExec(parser).run(false))
          .isInstanceOf(RuntimeException.class)
          .hasMessage("file '" + file.getAbsolutePath() + "' not parse-able");
    } finally {
      if (file != null) {
        file.deleteOnExit();
      }
    }
  }
}
