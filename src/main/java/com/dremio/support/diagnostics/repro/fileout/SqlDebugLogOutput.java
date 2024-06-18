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
package com.dremio.support.diagnostics.repro.fileout;

import com.dremio.support.diagnostics.repro.JobResult;
import com.dremio.support.diagnostics.repro.PdsSql;
import com.dremio.support.diagnostics.repro.SqlOutput;
import com.dremio.support.diagnostics.repro.VdsReference;
import com.dremio.support.diagnostics.repro.VdsSql;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class SqlDebugLogOutput implements SqlOutput {
  private final OutputStream outputStream;
  private final String sqlDebugLog;

  public SqlDebugLogOutput(String sqlDebugLog) throws IOException {
    this.outputStream = Files.newOutputStream(Paths.get(sqlDebugLog));
    this.sqlDebugLog = sqlDebugLog;
  }

  /**
   * provides a title so that consumers can know which strategy is running
   *
   * @return the name of this strategy
   */
  @Override
  public String getName() {
    return "SQL Debug Log " + this.sqlDebugLog;
  }

  /**
   * no op
   *
   * @param spaces spaces to output to destination, duplicates will be duplicated,
   * @return the result of the attempt
   */
  @Override
  public JobResult spaceOutput(Collection<String> spaces) {
    JobResult jobResult = new JobResult();
    jobResult.setSuccess(true);
    return jobResult;
  }

  /**
   * no op
   *
   * @param folders folders to output to destination, duplicates will be duplicated,
   * @return the result of the attempt
   */
  @Override
  public JobResult folderOutput(Collection<Collection<String>> folders) {
    JobResult jobResult = new JobResult();
    jobResult.setSuccess(true);
    return jobResult;
  }

  /**
   * writes out pds
   *
   * @param pdsSql pds data to a destination, duplicates will be duplicated
   * @return the result of an attempt
   */
  @Override
  public JobResult writePDSs(Collection<PdsSql> pdsSql) {
    List<String> added = new ArrayList<>();
    try {
      for (PdsSql pds : pdsSql) {
        added.add(pds.getTableName().toLowerCase(Locale.US));
        outputStream.write(String.format("%s%n", pds.getSql()).getBytes(StandardCharsets.UTF_8));
      }
      outputStream.flush();
    } catch (IOException ex) {
      JobResult result = new JobResult();
      result.added(added);
      result.setSuccess(false);
      result.setFailure(ex.getMessage());
      return result;
    }
    JobResult result = new JobResult();
    result.added(added);
    result.setSuccess(true);
    return result;
  }

  /**
   * writes out vds
   *
   * @param vdsSql vds data to a destination, duplicates will be duplicated
   * @return the result of an attempt
   */
  @Override
  public JobResult writeVDSs(Collection<VdsSql> vdsSql, final Collection<VdsReference> vdsInfo) {
    List<String> added = new ArrayList<>();
    try {
      for (VdsSql vds : vdsSql) {
        added.add(vds.getTableName().toLowerCase(Locale.US));
        outputStream.write(String.format("%s%n", vds.getSql()).getBytes(StandardCharsets.UTF_8));
      }
      outputStream.flush();
    } catch (IOException e) {
      JobResult result = new JobResult();
      result.added(added);
      result.setSuccess(false);
      result.setFailure(e.getMessage());
      return result;
    }
    JobResult result = new JobResult();
    result.added(added);
    result.setSuccess(true);
    return result;
  }

  /**
   * no op
   *
   * @param sources sources to be created at the destination, duplicates will be duplicated
   * @param defaultCtasFormat defaultCTASFormat for the source
   * @return the result of the attempt
   */
  @Override
  public JobResult sourceOutput(Collection<String> sources, Optional<String> defaultCtasFormat) {
    JobResult jobResult = new JobResult();
    jobResult.setSuccess(true);
    return jobResult;
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
  public void close() throws IOException {
    this.outputStream.flush();
    this.outputStream.close();
  }
}
