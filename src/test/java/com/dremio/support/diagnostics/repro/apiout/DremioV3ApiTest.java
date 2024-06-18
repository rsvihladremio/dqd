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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.support.diagnostics.shared.FileMaker;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DremioV3ApiTest {
  private static String baseURL;
  private static HttpAuth auth;
  private static ApiCall apiCall;
  private static FileMaker fileMaker;
  private static int timeoutSeconds = 500;
  private static final String token = "myToken";

  static void addJsonContentType(Map<String, String> map) {
    map.put("Content-Type", "application/json");
  }

  static void addAuthHeader(Map<String, String> catalogArgs) {
    catalogArgs.put("Authorization", "_dremio" + token);
  }

  static void addCompletedResponse(final Map<String, Object> statusResponseMap) {
    statusResponseMap.put("jobState", "COMPLETED");
  }

  static void setupWorkingLogin() throws IOException {
    baseURL = "http://localhost:9047";
    String username = "myUser";
    String password = "myPassword";
    auth = new HttpAuth(username, password);
    apiCall = mock(ApiCall.class);
    fileMaker = mock(FileMaker.class);
    Map<String, String> map = new HashMap<>();
    addJsonContentType(map);
    HttpApiResponse response = new HttpApiResponse();
    response.setMessage("");
    response.setResponseCode(200);
    Map<String, Object> responseJson = new HashMap<>();
    responseJson.put("token", token);
    response.setResponse(responseJson);
    Mockito.doReturn(response)
        .when(apiCall)
        .submitPost(new URL("http://localhost:9047/apiv2/login"), map, auth.toString());
  }

  @Nested
  static class HappPathTests {

    @BeforeEach
    void setup() throws IOException {
      setupWorkingLogin();
    }

    @Test
    void testCreateFolder() throws IOException {
      // to login first to get that out of the way of other when calls
      DremioApi api = new DremioV3Api(apiCall, auth, baseURL, fileMaker, timeoutSeconds);

      // setup test call
      api.createFolder(Arrays.asList("test", "folder", "mystuff"));
      Map<String, String> catalogArgs = new HashMap<>();
      addAuthHeader(catalogArgs);
      addJsonContentType(catalogArgs);
      verify(apiCall)
          .submitPost(
              new URL("http://localhost:9047/api/v3/catalog"),
              catalogArgs,
              "{\"path\":[\"test\",\"folder\",\"mystuff\"],\"entityType\":\"folder\"}");
    }

    @Test
    void testCreateFolderCreatesASpace() throws IOException {
      // to login first to get that out of the way of other when calls
      DremioApi api = new DremioV3Api(apiCall, auth, baseURL, fileMaker, timeoutSeconds);

      // do test call and setup response

      api.createSpace("test");
      Map<String, String> catalogArgs = new HashMap<>();
      addAuthHeader(catalogArgs);
      addJsonContentType(catalogArgs);
      verify(apiCall)
          .submitPost(
              new URL("http://localhost:9047/api/v3/catalog"),
              catalogArgs,
              "{\"entityType\":\"space\",\"name\":\"test\"}");
    }

    @Test
    void testCreateSource() throws IOException {
      DremioApi api = new DremioV3Api(apiCall, auth, baseURL, fileMaker, timeoutSeconds);

      // setup api response and test call
      HttpApiResponse response = new HttpApiResponse();
      response.setResponseCode(200);
      when(apiCall.submitPost(any(), any(), any())).thenReturn(response);
      Path tmpPath = Paths.get("tmp", "dremio-repro-nfs15986456995371384821");
      when(fileMaker.getNewDir()).thenReturn(tmpPath);

      api.createSource("myNFS", Optional.empty());

      // now verify the right set arguments is called
      URL url = new URL("http://localhost:9047/api/v3/catalog");
      Map<String, String> catalogArgs = new HashMap<>();
      addAuthHeader(catalogArgs);
      addJsonContentType(catalogArgs);
      String body =
          String.format(
              "{\"entityType\":\"source\",\"name\":\"myNFS\",\"type\":\"NAS\",\"config\""
                  + ":{\"path\":\"%s\"},\"metadataPolicy\":{\"namesRefreshMs\":3600000,\"authTTLMs\":86400000,\"deleteUnavailableDatasets\":true,"
                  + "\"datasetExpireAfterMs\":10800000,\"datasetUpdateMode\":\"PREFETCH_QUERIED\",\"datasetRefreshAfterMs\":3600000,\"autoPromoteDatasets\":true}}",
              tmpPath.toString().replace("\\", "\\\\")); // for windows we need to escape the \
      verify(apiCall).submitPost(url, catalogArgs, body);
    }
  }

  @Nested
  static class WhenJobIdIsReturned {
    private static DremioApiResponse apiResponse;
    private static String body;
    private static URL url;
    private static Map<String, String> catalogArgs;

    @BeforeAll
    static void initAll() throws IOException {
      setupWorkingLogin();
      DremioApi api = new DremioV3Api(apiCall, auth, baseURL, fileMaker, timeoutSeconds);

      // set up the post for sql, so we get back a valid response object
      final HttpApiResponse response = new HttpApiResponse();
      response.setResponseCode(200);
      final Map<String, Object> responseMap = new HashMap<>();
      final String id = "2bds-f323-1ejk";
      responseMap.put("id", id);
      response.setResponse(responseMap);
      url = new URL("http://localhost:9047/api/v3/sql");
      catalogArgs = new HashMap<>();
      addAuthHeader(catalogArgs);
      addJsonContentType(catalogArgs);
      body = "{\"sql\":\"SELECT * FROM bar\"}";
      when(apiCall.submitPost(any(), any(), any())).thenReturn(response);

      // now set up the status check call
      final HttpApiResponse statusResponse = new HttpApiResponse();
      statusResponse.setResponseCode(200);
      final Map<String, Object> statusResponseMap = new HashMap<>();
      addCompletedResponse(statusResponseMap);
      statusResponse.setResponse(statusResponseMap);
      when(apiCall.submitGet(new URL("http://localhost:9047/api/v3/job/" + id), catalogArgs))
          .thenReturn(statusResponse);

      // actually execute the sql
      apiResponse = api.runSQL("SELECT * FROM bar", "bar");
    }

    @Test
    void testRunSQLWasCalled() throws IOException {
      // verify my call happened as expected and matched the arguments
      verify(apiCall).submitPost(url, catalogArgs, body);
    }

    @Test
    void testNoError() {
      assertThat(apiResponse.getErrorMessage()).isNull();
    }

    @Test
    void testIsCreated() {
      assertThat(apiResponse.isCreated()).isTrue();
    }
  }

  @Nested
  static class RunSQLChecksJobStatusWithPendingStatus {

    private static URL getUrl;
    private static URL url;
    private static String body;
    private static Map<String, String> catalogArgs;
    private static DremioApiResponse apiResponse;

    @BeforeAll
    static void initAll() throws IOException {
      setupWorkingLogin();
      DremioApi api = new DremioV3Api(apiCall, auth, baseURL, fileMaker, timeoutSeconds);

      // set up the post for sql, so we get back a valid response object
      final HttpApiResponse response = new HttpApiResponse();
      response.setResponseCode(200);
      final Map<String, Object> responseMap = new HashMap<>();
      final String id = "2bds-f323-1ejk";
      responseMap.put("id", id);
      response.setResponse(responseMap);
      url = new URL("http://localhost:9047/api/v3/sql");
      catalogArgs = new HashMap<>();
      addAuthHeader(catalogArgs);
      addJsonContentType(catalogArgs);
      body = "{\"sql\":\"SELECT * FROM bar\"}";
      when(apiCall.submitPost(any(), any(), any())).thenReturn(response);

      // now set up the status check call to return 3 times then the 4 time return complete
      final HttpApiResponse statusResponse = new HttpApiResponse();
      statusResponse.setResponseCode(200);
      final Map<String, Object> statusResponseMap = new HashMap<>();
      statusResponseMap.put("jobState", "PENDING");
      statusResponse.setResponse(statusResponseMap);

      final HttpApiResponse complete = new HttpApiResponse();
      complete.setResponseCode(200);
      final Map<String, Object> completeMap = new HashMap<>();
      addCompletedResponse(completeMap);
      complete.setResponse(completeMap);
      getUrl = new URL("http://localhost:9047/api/v3/job/" + id);
      // first two times return PENDING third time return COMPLETED
      when(apiCall.submitGet(getUrl, catalogArgs))
          .thenReturn(statusResponse, statusResponse, complete);

      // actually execute the sql
      apiResponse = api.runSQL("SELECT * FROM bar", "bar");
    }

    @Test
    void testChecksApi3Times() throws IOException {
      // verify my call happened as expected and matched the arguments
      verify(apiCall, times(3)).submitGet(getUrl, catalogArgs);
    }

    @Test
    void testSubmitsRunSql() throws IOException {
      verify(apiCall).submitPost(url, catalogArgs, body);
    }

    @Test
    void testErrorMessageIsNull() {
      assertThat(apiResponse.getErrorMessage()).isNull();
    }

    @Test
    void testJobIsSuccessful() {
      assertThat(apiResponse.isCreated()).isTrue();
    }
  }

  @Nested
  static class TimeOutFiles {
    private static URL getUrl;
    private static URL url;
    private static String body;
    private static Map<String, String> catalogArgs;
    private static DremioApiResponse apiResponse;

    @BeforeAll
    static void initAll() throws IOException {
      setupWorkingLogin();
      DremioApi api = new DremioV3Api(apiCall, auth, baseURL, fileMaker, 1);

      // set up the post for sql, so we get back a valid response object
      final HttpApiResponse response = new HttpApiResponse();
      response.setResponseCode(200);
      final Map<String, Object> responseMap = new HashMap<>();
      final String id = "2bds-f323-1ejk";
      responseMap.put("id", id);
      response.setResponse(responseMap);
      url = new URL("http://localhost:9047/api/v3/sql");
      catalogArgs = new HashMap<>();
      addAuthHeader(catalogArgs);
      addJsonContentType(catalogArgs);
      final String sql = "create VDS myvds as SELECT * FROM abdc.bar where cat=1";
      body = String.format("{\"sql\":\"%s\"}", sql);
      when(apiCall.submitPost(any(), any(), any())).thenReturn(response);

      // now set up the status check call to return 3 times
      final HttpApiResponse statusResponse = new HttpApiResponse();
      statusResponse.setResponseCode(200);
      final Map<String, Object> statusResponseMap = new HashMap<>();
      statusResponseMap.put("jobState", "PENDING");
      statusResponse.setResponse(statusResponseMap);

      final HttpApiResponse complete = new HttpApiResponse();
      complete.setResponseCode(200);
      final Map<String, Object> completeMap = new HashMap<>();
      addCompletedResponse(completeMap);
      complete.setResponse(completeMap);
      getUrl = new URL("http://localhost:9047/api/v3/job/" + id);
      // first three times return PENDING even though the third _SHOULD_ _NOT_ be called
      when(apiCall.submitGet(getUrl, catalogArgs))
          .thenReturn(statusResponse, statusResponse, statusResponse);

      // actually execute the sql
      apiResponse = api.runSQL(sql, "myvds");
    }

    @Test
    void testSubmitGetCalledTwice() throws IOException {
      // verify my call happened as expected and matched the arguments
      // only 2 times since we hit a timeout
      verify(apiCall, times(2)).submitGet(getUrl, catalogArgs);
    }

    @Test
    void testSubmitPostCalledOnce() throws IOException {
      verify(apiCall).submitPost(url, catalogArgs, body);
    }

    @Test
    void testJobFailed() {
      assertThat(apiResponse.isCreated()).isFalse();
    }

    @Test
    void testJobShowsStatusOfJob() {
      assertThat(apiResponse.getErrorMessage()).isEqualTo("Response status is 'PENDING'");
    }
  }
}
