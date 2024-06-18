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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ExecTest {

  private static final String sql = "select * from a";
  private static final String sql2 = "select * from b";

  @Test
  void testSingleOutput() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      when(sqlOutput.writePDSs(pds)).thenReturn(success);
      when(sqlOutput.writeVDSs(emptyList(), emptyList())).thenReturn(success);
      when(sqlOutput.folderOutput(emptyList())).thenReturn(success);
      when(sqlOutput.spaceOutput(emptyList())).thenReturn(success);
      when(sqlOutput.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(0);
      verify(sqlOutput, times(1)).writePDSs(pds);
    }
  }

  @Test
  void testTwoSqlOutput() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput1 = mock(SqlOutput.class)) {
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      when(sqlOutput1.writePDSs(pds)).thenReturn(success);
      when(sqlOutput1.writeVDSs(emptyList(), emptyList())).thenReturn(success);
      when(sqlOutput1.folderOutput(emptyList())).thenReturn(success);
      when(sqlOutput1.spaceOutput(emptyList())).thenReturn(success);
      when(sqlOutput1.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);
      try (SqlOutput sqlOutput2 = mock(SqlOutput.class)) {
        when(sqlOutput2.writePDSs(pds)).thenReturn(success);
        when(sqlOutput2.writeVDSs(emptyList(), emptyList())).thenReturn(success);
        when(sqlOutput2.folderOutput(emptyList())).thenReturn(success);
        when(sqlOutput2.spaceOutput(emptyList())).thenReturn(success);
        when(sqlOutput2.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);

        Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput1, sqlOutput2);
        int code = exec.run().getErrorCode();
        assertThat(code).isEqualTo(0);
        verify(sqlOutput1, times(1)).writePDSs(pds);
        verify(sqlOutput2, times(1)).writePDSs(pds);
      }
    }
  }

  @Test
  void testNoSqlOutput() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();

    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);

    ReproProfileParser parser = spy(ReproProfileParser.class);
    when(parser.parsePDSs(profileJSON)).thenReturn(emptyList());

    try (SqlOutput sqlOutput = spy(SqlOutput.class)) {

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(2);
    }
  }

  @Test
  void testAreNoPdsFound() throws IOException {
    ProfileProvider profile = spy(ProfileProvider.class);
    ReproProfileParser parser = spy(ReproProfileParser.class);

    Exec exec = new Exec(Optional.empty(), profile, parser);
    int code = exec.run().getErrorCode();
    assertThat(code).isEqualTo(1);
  }

  @Test
  void testSingleOutputFolderFails() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput = spy(SqlOutput.class)) {
      when(sqlOutput.getName()).thenReturn("API Report");
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      JobResult failure = new JobResult();
      failure.setSuccess(false);
      failure.setFailure("my bad didn't work");
      when(sqlOutput.writePDSs(pds)).thenReturn(success);
      when(sqlOutput.writeVDSs(emptyList(), emptyList())).thenReturn(success);
      when(sqlOutput.folderOutput(emptyList())).thenReturn(failure);
      when(sqlOutput.spaceOutput(emptyList())).thenReturn(success);
      when(sqlOutput.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(1);
    }
  }

  @Test
  void testSingleOutputSpacesFails() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput = spy(SqlOutput.class)) {
      when(sqlOutput.getName()).thenReturn("Custom Report");
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      JobResult failure = new JobResult();
      failure.setSuccess(false);
      failure.setFailure("failure making space");
      when(sqlOutput.writePDSs(pds)).thenReturn(success);
      when(sqlOutput.writeVDSs(emptyList(), emptyList())).thenReturn(success);
      when(sqlOutput.folderOutput(emptyList())).thenReturn(success);
      when(sqlOutput.spaceOutput(emptyList())).thenReturn(failure);
      when(sqlOutput.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(1);
    }
  }

  @Test
  void testSingleOutputSourceFails() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput = spy(SqlOutput.class)) {
      when(sqlOutput.getName()).thenReturn("My Custom Report");
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      JobResult failure = new JobResult();
      failure.setSuccess(false);
      failure.setFailure("my bad");
      when(sqlOutput.writePDSs(pds)).thenReturn(success);
      when(sqlOutput.writeVDSs(emptyList(), emptyList())).thenReturn(success);
      when(sqlOutput.folderOutput(emptyList())).thenReturn(success);
      when(sqlOutput.spaceOutput(emptyList())).thenReturn(success);
      when(sqlOutput.sourceOutput(emptyList(), Optional.empty())).thenReturn(failure);

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(1);
    }
  }

  @Test
  void testSingleOutputPdsFails() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput = spy(SqlOutput.class)) {
      when(sqlOutput.getName()).thenReturn("API Report");
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      JobResult failure = new JobResult();
      failure.setSuccess(false);
      failure.setFailure("failed writing pds");
      when(sqlOutput.writePDSs(pds)).thenReturn(failure);
      when(sqlOutput.writeVDSs(emptyList(), emptyList())).thenReturn(success);
      when(sqlOutput.folderOutput(emptyList())).thenReturn(success);
      when(sqlOutput.spaceOutput(emptyList())).thenReturn(success);
      when(sqlOutput.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(1);
    }
  }

  @Test
  void testSingleOutputVdsFails() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    List<PdsSql> pds = Arrays.asList(new PdsSql("a", sql), new PdsSql("b", sql2));
    when(parser.parsePDSs(profileJSON)).thenReturn(pds);
    try (SqlOutput sqlOutput = spy(SqlOutput.class)) {
      when(sqlOutput.getName()).thenReturn("Report for VDS");
      JobResult success = new JobResult();
      success.added(pds.stream().map(PdsSql::getSql).collect(Collectors.toList()));
      success.setSuccess(true);
      JobResult failure = new JobResult();
      failure.setSuccess(false);
      failure.setFailure("failed writing VDS");
      when(sqlOutput.writePDSs(pds)).thenReturn(success);
      when(sqlOutput.writeVDSs(emptyList(), emptyList())).thenReturn(failure);
      when(sqlOutput.folderOutput(emptyList())).thenReturn(success);
      when(sqlOutput.spaceOutput(emptyList())).thenReturn(success);
      when(sqlOutput.sourceOutput(emptyList(), Optional.empty())).thenReturn(success);

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(1);
    }
  }

  @Test
  void testHandlesExceptionParsing() throws IOException {
    final ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenThrow(new IOException("failure!!!"));
    final ReproProfileParser parser = spy(ReproProfileParser.class);
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {
      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(3);
    }
  }

  @Test
  void testHandlesExceptionReadingPds() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    when(parser.parsePDSs(profileJSON)).thenThrow(new RuntimeException("unable to read PDS"));
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {
      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(3);
    }
  }

  @Test
  void testHandlesExceptionReadingVds() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    when(parser.parsePDSs(profileJSON))
        .thenReturn(Collections.singletonList(new PdsSql("abc", "select 1")));
    when(parser.parseVDSs(profileJSON)).thenThrow(new RuntimeException("unable to parse VDS"));
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {
      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(3);
    }
  }

  @Test
  void testHandlesExceptionReadingSpaces() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    when(parser.parsePDSs(profileJSON))
        .thenReturn(Collections.singletonList(new PdsSql("abc", "select 1")));
    when(parser.parseSpaces(profileJSON)).thenThrow(new RuntimeException("unable to parse Spaces"));
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {

      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(3);
    }
  }

  @Test
  void testHandlesExceptionReadingFolders() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    when(parser.parsePDSs(profileJSON))
        .thenReturn(Collections.singletonList(new PdsSql("abc", "select 1")));
    when(parser.parseFolders(profileJSON))
        .thenThrow(new RuntimeException("unable to parse folders"));
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {
      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(3);
    }
  }

  @Test
  void testHandlesExceptionReadingSources() throws IOException {
    ProfileJSON profileJSON = new ProfileJSON();
    ProfileProvider profile = spy(ProfileProvider.class);
    when(profile.getProfile()).thenReturn(profileJSON);
    ReproProfileParser parser = spy(ReproProfileParser.class);
    when(parser.parsePDSs(profileJSON))
        .thenReturn(Collections.singletonList(new PdsSql("abc", "select 1")));
    when(parser.parseSources(profileJSON))
        .thenThrow(new RuntimeException("unable to parse sources"));
    try (SqlOutput sqlOutput = mock(SqlOutput.class)) {
      Exec exec = new Exec(Optional.empty(), profile, parser, sqlOutput);
      int code = exec.run().getErrorCode();
      assertThat(code).isEqualTo(3);
    }
  }
}
