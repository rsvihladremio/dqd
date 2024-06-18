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
package com.dremio.support.diagnostics.repro.consoleout;

import com.dremio.support.diagnostics.repro.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConsoleOutput implements SqlOutput {

  /**
   * provides a title so that consumers can know which strategy is running
   *
   * @return the human friendly name of the console strategy
   */
  @Override
  public String getName() {
    return "Console";
  }

  /**
   * writes out the list of spaces to the console
   *
   * @param spaces spaces to write out
   * @return the result, should always be successful
   */
  @Override
  public JobResult spaceOutput(Collection<String> spaces) {
    System.out.println("spaces to create");
    System.out.println("----------------------------");
    for (String space : spaces) {
      System.out.println(space);
    }
    System.out.println();
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(new ArrayList<>(spaces));
    return result;
  }

  /**
   * this writes out the list of folders to the console
   *
   * @param folders folders to write out to the console
   * @return the result, should always be successful
   */
  @Override
  public JobResult folderOutput(Collection<Collection<String>> folders) {
    System.out.println("folders to create");
    System.out.println("-----------------");
    List<String> foldersAdded = new ArrayList<>();
    for (Collection<String> folder : folders) {
      final String fullFolder = String.join("/", folder);
      foldersAdded.add(fullFolder);
      System.out.println(fullFolder);
    }
    System.out.println();
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(foldersAdded);
    return result;
  }

  /**
   * writes out the pds list to the console
   *
   * @param pdsSql pds list to write out
   * @return result, should always succeed
   */
  @Override
  public JobResult writePDSs(Collection<PdsSql> pdsSql) {
    System.out.println("pds sql to create");
    System.out.println("-----------------");
    for (PdsSql pds : pdsSql) {
      System.out.println(pds.getSql());
    }
    System.out.println();
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(pdsSql.stream().map(PdsSql::getSql).collect(Collectors.toList()));
    return result;
  }

  /**
   * this writes out VDS to the console and is mostly just a debugging method
   *
   * @param vdsSql the vds list to write out to the console
   * @return a job result, but it is expected this will always succeed
   */
  @Override
  public JobResult writeVDSs(
      final Collection<VdsSql> vdsSql, final Collection<VdsReference> vdsReferences) {
    System.out.println("vds sql to create");
    System.out.println("-----------------");
    for (final VdsSql e : vdsSql) {
      System.out.println(e.getSql());
    }
    for (final VdsReference e : vdsReferences) {
      System.out.println(e);
    }
    System.out.println();
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(vdsSql.stream().map(VdsSql::getSql).collect(Collectors.toList()));
    return result;
  }

  /**
   * writes out a list of sources to the console
   *
   * @param sources source to write out
   * @return result, should always succeed
   */
  @Override
  public JobResult sourceOutput(Collection<String> sources, Optional<String> defaultCtasFormat) {
    System.out.println("sources to create");
    System.out.println("-----------------");
    for (String source : sources) {
      if (defaultCtasFormat.isPresent()) {
        System.out.println(source + " with format: " + defaultCtasFormat.get());
      } else {
        System.out.println(source);
      }
    }
    System.out.println();
    JobResult result = new JobResult();
    result.setSuccess(true);
    result.added(sources);
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
  public void close() throws IOException {
    // noop
  }
}
