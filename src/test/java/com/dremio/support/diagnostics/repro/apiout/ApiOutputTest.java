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
package com.dremio.support.diagnostics.repro.apiout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.dremio.support.diagnostics.repro.JobResult;
import com.dremio.support.diagnostics.repro.PdsSql;
import com.dremio.support.diagnostics.repro.VdsSql;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ApiOutputTest {

  @Test
  void getName() throws IOException {
    DremioApi dremioApi = spy(DremioApi.class);
    when(dremioApi.getUrl()).thenReturn("http://localhost:9047");
    try (ApiOutput api = new ApiOutput(dremioApi)) {
      assertThat(api.getName()).isEqualTo("Dremio REST API to http://localhost:9047");
    }
  }

  @Test
  void spaceOutput() throws IOException {
    DremioApi dremioApi = mock(DremioApi.class);
    String space1 = "space1";
    String space2 = "space2";
    DremioApiResponse response = new DremioApiResponse();
    response.setCreated(true);
    when(dremioApi.createSpace(space1)).thenReturn(response);
    when(dremioApi.createSpace(space2)).thenReturn(response);
    try (ApiOutput api = new ApiOutput(dremioApi)) {
      JobResult result = new JobResult();
      result.setSuccess(true);
      result.added(Arrays.asList(space1, space2));
      assertThat(api.spaceOutput(Arrays.asList(space1, space2))).isEqualTo(result);
      verify(dremioApi, times(1)).createSpace(space1);
      verify(dremioApi, times(1)).createSpace(space2);
    }
  }

  @Test
  void folderOutput() throws IOException {
    DremioApi dremioApi = mock(DremioApi.class);
    List<String> folder1 = Arrays.asList("a", "b");
    List<String> folder2 = Arrays.asList("a", "b", "c");
    DremioApiResponse response = new DremioApiResponse();
    response.setCreated(true);
    when(dremioApi.createFolder(folder1)).thenReturn(response);
    when(dremioApi.createFolder(folder2)).thenReturn(response);
    try (ApiOutput api = new ApiOutput(dremioApi)) {
      JobResult result = new JobResult();
      result.setSuccess(true);
      result.added(Arrays.asList("[ a, b ]", "[ a, b, c ]"));
      assertThat(api.folderOutput(Arrays.asList(folder1, folder2))).isEqualTo(result);
    }
  }

  @Test
  void writePDSs() throws IOException {
    DremioApi dremioApi = mock(DremioApi.class);
    PdsSql pds1 =
        new PdsSql(
            "source.table1",
            "CREATE TABLE source.table1 SELECT * FROM (1, 2) as t(\"row1\", \"row2\")");
    PdsSql pds2 =
        new PdsSql(
            "source.table2",
            "CREATE TABLE source.table2 SELECT * FROM (1, 2) as t(\"row1\", \"row2\")");
    DremioApiResponse response = new DremioApiResponse();
    response.setCreated(true);
    when(dremioApi.runSQL(pds1.getSql(), pds1.getTableName())).thenReturn(response);
    when(dremioApi.runSQL(pds2.getSql(), pds2.getTableName())).thenReturn(response);
    try (ApiOutput api = new ApiOutput(dremioApi)) {
      JobResult result = new JobResult();
      result.setSuccess(true);
      result.added(Arrays.asList(pds1.getTableName(), pds2.getTableName()));
      assertThat(api.writePDSs(Arrays.asList(pds1, pds2))).isEqualTo(result);
      verify(dremioApi, times(1)).runSQL(pds1.getSql(), pds1.getTableName());
      verify(dremioApi, times(1)).runSQL(pds2.getSql(), pds2.getTableName());
    }
  }

  @Test
  void writeVDSs() throws IOException {
    DremioApi dremioApi = mock(DremioApi.class);
    VdsSql vds1 =
        new VdsSql(
            "space.vds1",
            "CREATE Vds space.vds1 as (SELECT * FROM source.table1)",
            new String[] {"source.table1"});
    VdsSql vds2 =
        new VdsSql(
            "space.vds2",
            "CREATE Vds space.vds2 as (SELECT * FROM source.table2)",
            new String[] {"source.table2"});
    DremioApiResponse response = new DremioApiResponse();
    response.setCreated(true);
    when(dremioApi.runSQL(vds1.getSql(), vds1.getTableName())).thenReturn(response);
    when(dremioApi.runSQL(vds2.getSql(), vds2.getTableName())).thenReturn(response);
    try (ApiOutput api = new ApiOutput(dremioApi)) {
      JobResult result = new JobResult();
      result.setSuccess(true);
      result.added(Arrays.asList(vds1.getTableName(), vds2.getTableName()));
      assertThat(api.writeVDSs(Arrays.asList(vds1, vds2), new ArrayList<>())).isEqualTo(result);
      verify(dremioApi, times(1)).runSQL(vds1.getSql(), vds1.getTableName());
      verify(dremioApi, times(1)).runSQL(vds2.getSql(), vds2.getTableName());
    }
  }

  @Test
  void sourceOutput() throws IOException {
    DremioApi dremioApi = mock(DremioApi.class);
    String source1 = "source1";
    String source2 = "source2";
    DremioApiResponse response = new DremioApiResponse();
    response.setCreated(true);
    when(dremioApi.createSource(source1, Optional.empty())).thenReturn(response);
    when(dremioApi.createSource(source2, Optional.empty())).thenReturn(response);
    try (ApiOutput api = new ApiOutput(dremioApi)) {
      JobResult result = new JobResult();
      result.setSuccess(true);
      result.added(Arrays.asList("source1", "source2"));
      assertThat(api.sourceOutput(Arrays.asList(source1, source2), Optional.empty()))
          .isEqualTo(result);
      verify(dremioApi, times(1)).createSource(source1, Optional.empty());
      verify(dremioApi, times(1)).createSource(source2, Optional.empty());
    }
  }
}
