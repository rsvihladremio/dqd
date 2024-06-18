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

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public interface DremioApi {
  /**
   * createSpace creates a space in dremio
   *
   * @param space space to create
   * @return status of request, if it was created or not, if there was an error and if it was a
   *     folder or a space
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   */
  DremioApiResponse createSpace(String space) throws IOException;

  /**
   * createFolder will create a folder out of the paths provided
   *
   * @param folderPath paths to create
   * @return status of request, if it was created or not, if there was an error and if it was a
   *     folder or a space
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   */
  DremioApiResponse createFolder(Collection<String> folderPath) throws IOException;

  /**
   * createSource will make an api call to dremio and create an NFS space. The space has auto
   * promotion enabled and uses a temp directory
   *
   * @param sourceName nfs source to create
   * @param defaultCtasFormat defaultCTASFormat for the source
   * @return status of request, if it was created or not and if there was an error
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   */
  DremioApiResponse createSource(String sourceName, Optional<String> defaultCtasFormat)
      throws IOException;

  /**
   * runs a sql statement against the rest API
   *
   * @param sql sql string to submit to dremio
   * @param table table that the operation is being performed on, this is just for book keeping
   * @return the result of the job
   * @throws IOException occurs when the underlying apiCall does, typically a problem with handling
   *     of the body
   */
  DremioApiResponse runSQL(String sql, String table) throws IOException;

  /**
   * The http URL for the dremio server
   *
   * @return return the url used to access Dremio
   */
  String getUrl();
}
