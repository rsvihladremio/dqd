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

import com.dremio.support.diagnostics.repro.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** ApiOutput orchestrates making api calls to an api */
public class ApiOutput implements SqlOutput {
  private final DremioApi api;

  public ApiOutput(DremioApi api) {
    this.api = api;
  }

  /**
   * the title of the ApiOutput class
   *
   * @return the human friendly title of the output strategy
   */
  @Override
  public String getName() {
    return "Dremio REST API to " + api.getUrl();
  }

  /**
   * @param spaces space to create
   * @return the api result if it failed or not
   */
  @Override
  public JobResult spaceOutput(Collection<String> spaces) {
    JobResult result = new JobResult();
    result.setSuccess(true);
    List<String> spacesAdded = new ArrayList<>();
    for (String space : spaces) {
      try {
        System.out.printf("making space: %s%n", space);
        DremioApiResponse response = this.api.createSpace(space);
        if (!response.isCreated()) {
          result.setSuccess(false);
          // go ahead and exit now that we failed
          break;
        }
        spacesAdded.add(space);
      } catch (IOException e) {
        result.setSuccess(false);
        result.setFailure(e.getMessage());
        // go ahead and exit now that we failed
        break;
      }
    }
    result.added(spacesAdded);
    return result;
  }

  /**
   * @param folders the path to create
   * @return the api call result
   */
  @Override
  public JobResult folderOutput(Collection<Collection<String>> folders) {
    JobResult result = new JobResult();
    result.setSuccess(true);
    List<String> foldersAdded = new ArrayList<>();
    for (Collection<String> folder : folders) {
      try {
        System.out.printf("making folder: %s%n", folder);
        DremioApiResponse response = this.api.createFolder(folder);
        if (!response.isCreated()) {
          result.setSuccess(false);
          // go ahead and exit now that we failed
          break;
        }
        foldersAdded.add(String.format("[ %s ]", String.join(", ", folder)));
      } catch (IOException e) {
        result.setSuccess(false);
        result.setFailure(e.getMessage());
        // go ahead and exit now that we failed
        break;
      }
    }
    result.added(foldersAdded);
    return result;
  }

  /**
   * @param pdsSql the sql to use to create the pds
   * @return the api result
   */
  @Override
  public JobResult writePDSs(Collection<PdsSql> pdsSql) {
    JobResult result = new JobResult();
    result.setSuccess(true);
    List<String> pdsAdded = new ArrayList<>();
    for (PdsSql sql : pdsSql) {
      try {
        System.out.printf("making pds: %s%n", sql.getTableName());
        DremioApiResponse response = this.api.runSQL(sql.getSql(), sql.getTableName());
        if (!response.isCreated()) {
          result.setSuccess(false);
          result.setFailure(response.getErrorMessage());
          // go ahead and exit now that we failed
          break;
        }
        pdsAdded.add(sql.getTableName());
      } catch (IOException e) {
        result.setSuccess(false);
        result.setFailure(e.getMessage());
        // go ahead and exit now that we failed
        break;
      }
    }
    result.added(pdsAdded);
    return result;
  }

  /**
   * @param vdsSql list of vds to create
   * @param vdsReferenceInfo information frmo the creation of the VDSs and their order
   * @return the api result
   */
  @Override
  public JobResult writeVDSs(
      final Collection<VdsSql> vdsSql, final Collection<VdsReference> vdsReferenceInfo) {
    JobResult result = new JobResult();
    result.setSuccess(true);
    List<String> vdsAdded = new ArrayList<>();

    for (VdsSql sql : vdsSql) {
      try {
        System.out.printf("making vds: %s%n", sql.getTableName());
        DremioApiResponse response = this.api.runSQL(sql.getSql(), sql.getTableName());
        if (!response.isCreated()) {
          result.setSuccess(false);
          // go ahead and exit now that we failed
          break;
        }
        vdsAdded.add(sql.getTableName());
      } catch (IOException e) {
        result.setSuccess(false);
        result.setFailure(e.getMessage());
        // go ahead and exit now that we failed
        break;
      }
    }
    result.added(vdsAdded);
    return result;
  }

  /**
   * @param sources the list of sources to create
   * @return the api all results
   */
  @Override
  public JobResult sourceOutput(Collection<String> sources, Optional<String> defaultCtasFormat) {
    JobResult result = new JobResult();
    result.setSuccess(true);
    List<String> pdsAdded = new ArrayList<>();
    for (String source : sources) {
      try {
        System.out.printf("making source: %s%n", source);
        DremioApiResponse response = this.api.createSource(source, defaultCtasFormat);
        if (!response.isCreated()) {
          result.setSuccess(false);
          // go ahead and exit now that we failed
          break;
        }
        pdsAdded.add(source);
      } catch (IOException e) {
        result.setSuccess(false);
        result.setFailure(e.getMessage());
        // go ahead and exit now that we failed
        break;
      }
    }
    result.added(pdsAdded);
    return result;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If the stream is
   * already closed then invoking this method has no effect.
   *
   * <p>As noted in {@link AutoCloseable#close()}, cases where the close may fail require careful
   * attention. It is strongly advised to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {}
}
