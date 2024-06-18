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

import com.dremio.support.diagnostics.shared.FileMaker;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.logging.Logger;

/** DremioApi business logic for interacting with the dremio rest api */
public class DremioV3Api implements DremioApi {

  /** unmodifiable map of base headers used in all requests that are authenticated */
  private final Map<String, String> baseHeaders;

  // base url for the api typically http/https hostname and port. Does not include the ending /
  private final String baseUrl;
  // the actual http implementation
  private final ApiCall apiCall;

  private static final Logger logger = Logger.getLogger(DremioV3Api.class.getName());
  private final FileMaker fileMaker;
  private final int timeoutSeconds;

  /**
   * DremioApi provides the business logic for making API calls. The constructor will connect to the
   * auth api, so we can store the auth token for subsequent requests.
   *
   * @param apiCall implementation that makes the http calls
   * @param auth generates a valid auth header
   * @param baseUrl base url for the api typically http/https hostname and port. Does not include
   *     the ending /
   * @param fileMaker creates files for nfs data sources
   * @param timeoutSeconds how long to try runSQL operations
   * @throws IOException throws when unable to read the response body or unable to attach a request
   *     body
   */
  public DremioV3Api(
      ApiCall apiCall, HttpAuth auth, String baseUrl, FileMaker fileMaker, int timeoutSeconds)
      throws IOException {
    this.apiCall = apiCall;
    this.fileMaker = fileMaker;
    this.timeoutSeconds = timeoutSeconds;
    Map<String, String> headers = new HashMap<>();
    // working with json
    headers.put("Content-Type", "application/json");
    // v2 login api
    URL url = new URL(baseUrl + "/apiv2/login");
    // auth string from username and password is the body
    HttpApiResponse response = apiCall.submitPost(url, headers, auth.toString());
    // the response needs to contain the token we will use for subsequent requests
    if (response == null
        || response.getResponse() == null
        || !response.getResponse().containsKey("token")) {
      throw new RuntimeException(
          String.format("token was not contained in the response '%s'", response));
    }
    // now that we know the token is there add it
    final String token = String.format("_dremio%s", response.getResponse().get("token"));
    Map<String, String> baseHeaders = new HashMap<>();
    baseHeaders.put("Authorization", token);
    baseHeaders.put("Content-Type", "application/json");
    this.baseHeaders = Collections.unmodifiableMap(baseHeaders);
    this.baseUrl = baseUrl;
  }

  /**
   * createSpace creates a space in dremio
   *
   * @param space space to create
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   * @return status of request, if it was created or not, if there was an error and if it was a
   *     folder or a space
   */
  @Override
  public DremioApiResponse createSpace(String space) throws IOException {
    URL url = new URL(this.baseUrl + "/api/v3/catalog");
    Map<String, Object> params = new HashMap<>();
    DremioApiResponse status = new DremioApiResponse();
    params.put("entityType", "space");
    params.put("name", space);
    String json = new ObjectMapper().writeValueAsString(params);
    try {
      HttpApiResponse response = apiCall.submitPost(url, this.baseHeaders, json);
      if (response != null && response.getResponseCode() == 409) {
        logger.info(() -> String.format("space %s already created. skipping%n", space));
        status.setCreated(true);
        return status;
      }
      if (response == null || response.getResponseCode() != 200) {
        String errorMessage = tryParseError(response);
        if (errorMessage == null) {
          errorMessage = String.format("unexpected response '%s'", response);
        }
        status.setErrorMessage(errorMessage);
        status.setCreated(false);
        return status;
      }
    } catch (Exception ex) {
      logger.warning(() -> String.format("error creating space %s: %s", space, ex.getMessage()));
      status.setErrorMessage(ex.getMessage());
      status.setCreated(false);
      return status;
    }
    status.setCreated(true);
    return status;
  }

  /**
   * createFolder when there is only a top level path, create a space, when the path is nested, it
   * instead will create a folder
   *
   * @param folderPath paths to create
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   * @return status of request, if it was created or not, if there was an error and if it was a
   *     folder or a space
   */
  @Override
  public DremioApiResponse createFolder(Collection<String> folderPath) throws IOException {
    URL url = new URL(this.baseUrl + "/api/v3/catalog");
    Map<String, Object> params = new HashMap<>();
    DremioApiResponse status = new DremioApiResponse();
    // top level path assume is a space
    // everything else
    params.put("entityType", "folder");
    params.put("path", folderPath);
    // avoid the calculation unless logging is enabled
    logger.fine(() -> String.format("folder path is: [%s]", String.join(", ", folderPath)));
    String json = new ObjectMapper().writeValueAsString(params);
    try {
      HttpApiResponse response = apiCall.submitPost(url, this.baseHeaders, json);
      if (response != null && response.getResponseCode() == 409) {
        logger.info(() -> String.format("folder %s already created. skipping", folderPath));
        status.setCreated(true);
        return status;
      }
      if (response == null || response.getResponseCode() != 200) {
        String errorMessage = tryParseError(response);
        if (errorMessage == null) {
          errorMessage = String.format("unexpected response '%s'", response);
        }
        status.setErrorMessage(errorMessage);
        status.setCreated(false);
        return status;
      }
    } catch (Exception ex) {
      logger.warning(
          () ->
              String.format(
                  "error creating folder %s: %s", String.join(", ", folderPath), ex.getMessage()));
      status.setErrorMessage(ex.getMessage());
      status.setCreated(false);
      return status;
    }
    status.setCreated(true);
    return status;
  }

  /**
   * createSource will make an api call to dremio and create an NFS space. The space has auto
   * promotion enabled and uses a temp directory
   *
   * @param sourceName nfs source to create
   * @param defaultCTASFormat defaultCTASFormat for the source
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   * @return status of request, if it was created or not and if there was an error
   */
  @Override
  public DremioApiResponse createSource(String sourceName, Optional<String> defaultCTASFormat)
      throws IOException {
    if (sourceName == null) {
      throw new RuntimeException("cannot have a source name of null");
    }
    URL url = new URL(this.baseUrl + "/api/v3/catalog");
    Map<String, Object> params = new HashMap<>();
    Path nasPath = this.fileMaker.getNewDir();
    params.put("entityType", "source");
    // cannot use quotes in source name
    params.put("name", sourceName.replace("\"", ""));
    params.put("type", "NAS");
    Map<String, Object> config = new HashMap<>();
    config.put("path", nasPath.toString());
    if (defaultCTASFormat.isPresent()) {
      config.put("defaultCtasFormat", defaultCTASFormat.get());
    }
    params.put("config", config);
    Map<String, Object> metaDataPolicy = new HashMap<>();
    metaDataPolicy.put("authTTLMs", 86400000);
    metaDataPolicy.put("namesRefreshMs", 3600000);
    metaDataPolicy.put("datasetRefreshAfterMs", 3600000);
    metaDataPolicy.put("datasetExpireAfterMs", 10800000);
    metaDataPolicy.put("datasetUpdateMode", "PREFETCH_QUERIED");
    metaDataPolicy.put("deleteUnavailableDatasets", true);
    metaDataPolicy.put("autoPromoteDatasets", true);
    params.put("metadataPolicy", metaDataPolicy);
    String json = new ObjectMapper().writeValueAsString(params);
    DremioApiResponse status = new DremioApiResponse();
    try {
      HttpApiResponse response = apiCall.submitPost(url, this.baseHeaders, json);
      if (response != null && response.getResponseCode() == 409) {
        logger.info(() -> String.format("source %s already created. skipping%n", sourceName));
        status.setCreated(true);
        return status;
      }
      if (response == null || response.getResponse() == null || response.getResponseCode() != 200) {
        String errorMessage = tryParseError(response);
        if (errorMessage == null) {
          errorMessage = String.format("unexpected response '%s'", response);
        }
        status.setErrorMessage(errorMessage);
        status.setCreated(false);
        return status;
      }
    } catch (Exception ex) {
      logger.warning(
          () -> String.format("error creating source %s: %s", sourceName, ex.getMessage()));
      status.setErrorMessage(ex.getMessage());
      status.setCreated(false);
      return status;
    }
    status.setCreated(true);
    return status;
  }

  /**
   * checkJobStatus is useful for seeing if a sql operation is complete and if it succeeded
   *
   * @param jobId job idea to check
   * @return the job state, which is just a single word
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   */
  private JobStatusResponse checkJobStatus(String jobId) throws IOException {
    // check for empty job id
    if (jobId == null || jobId.trim().isEmpty()) {
      throw new InvalidParameterException("jobId cannot be empty");
    }
    // v3 job api
    URL url = new URL(this.baseUrl + "/api/v3/job/" + jobId);
    // setup headers
    HttpApiResponse response = apiCall.submitGet(url, this.baseHeaders);
    // jobState is the necessary key
    if (response == null
        || response.getResponse() == null
        || !response.getResponse().containsKey("jobState")) {
      String error = tryParseError(response);
      if (error != null) {
        JobStatusResponse jobStatusResponse = new JobStatusResponse();
        jobStatusResponse.setStatus("UNKNOWN");
        jobStatusResponse.setMessage(error);
        return jobStatusResponse;
      }
      return null;
    }
    Object jobState = response.getResponse().get("jobState");
    if (jobState == null) {
      JobStatusResponse jobStatus = new JobStatusResponse();
      jobStatus.setStatus("UNKNOWN");
      return jobStatus;
    }
    // for failed jobs
    if ("FAILED".equals(jobState)) {
      String error =
          String.format("error message for job was %s", response.getResponse().get("errorMessage"));
      JobStatusResponse jobStatusResponse = new JobStatusResponse();
      jobStatusResponse.setStatus("FAILED");
      jobStatusResponse.setMessage(error);
      return jobStatusResponse;
    }
    String status = jobState.toString();
    JobStatusResponse jobStatus = new JobStatusResponse();
    jobStatus.setStatus(status);
    return jobStatus;
  }

  /**
   * runs a sql statement against the rest API
   *
   * @param sql sql string to submit to dremio
   * @return the result of the job
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   */
  @Override
  public DremioApiResponse runSQL(String sql, String table) throws IOException {
    if (sql == null || sql.trim().isEmpty()) {
      throw new InvalidParameterException("jobId cannot be empty");
    }
    URL url = new URL(baseUrl + "/api/v3/sql");
    Map<String, String> params = new HashMap<>();
    params.put("sql", sql);
    String json = new ObjectMapper().writeValueAsString(params);
    HttpApiResponse response = apiCall.submitPost(url, this.baseHeaders, json);
    if (response == null
        || response.getResponse() == null
        || !response.getResponse().containsKey("id")) {
      String errorMessage = tryParseError(response);
      if (errorMessage == null) {
        errorMessage = String.format("id was not contained in the response '%s'", response);
      }

      DremioApiResponse failed = new DremioApiResponse();
      failed.setCreated(false);
      failed.setErrorMessage(errorMessage);
      return failed;
    }
    JobStatusResponse status = new JobStatusResponse();
    status.setStatus("UNKNOWN");
    Instant timeout = Instant.now().plus(timeoutSeconds, ChronoUnit.SECONDS);
    while (!Instant.now().isAfter(timeout)) {
      String jobId = String.valueOf(response.getResponse().get("id"));
      status = this.checkJobStatus(jobId);
      if (status == null) {
        continue;
      }
      if ("COMPLETED".equals(status.getStatus())) {
        DremioApiResponse success = new DremioApiResponse();
        success.setCreated(true);
        return success;
      }
      if ("FAILED".equals(status.getStatus())) {
        if (status.getMessage().contains("already exists.")) {
          logger.info(() -> String.format("table %s already created. skipping%n", table));
          DremioApiResponse alreadyExists = new DremioApiResponse();
          alreadyExists.setCreated(true);
          return alreadyExists;
        }
        DremioApiResponse success = new DremioApiResponse();
        success.setCreated(false);
        success.setErrorMessage(String.format("Reponse status is '%s'", status.getMessage()));
        return success;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    DremioApiResponse failed = new DremioApiResponse();
    failed.setCreated(false);
    if (status != null) {
      failed.setErrorMessage(String.format("Response status is '%s'", status.getStatus()));
    } else {
      failed.setErrorMessage("unknown error");
    }
    return failed;
  }

  /**
   * @return return the url used to access Dremio
   */
  @Override
  public String getUrl() {
    return this.baseUrl;
  }

  private String tryParseError(HttpApiResponse response) {
    if (response != null
        && response.getResponse() != null
        && response.getResponse().containsKey("errorMessage")) {
      return String.valueOf(response.getResponse().get("errorMessage"));
    }
    return null;
  }
}
