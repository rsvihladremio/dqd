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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/** provides interface for outputing the results of the profile analysis to various outputs. */
public interface SqlOutput extends Closeable {
  /**
   * provides a title so that consumers can know which strategy is running
   *
   * @return the name of the SqlOutput strategy
   */
  String getName();

  /**
   * writes out spaces
   *
   * @param spaces spaces to output to destination, duplicates will be duplicated,
   * @return the result of the attempt
   * @throws IOException when a file system or remote api is not available
   */
  JobResult spaceOutput(Collection<String> spaces) throws IOException;

  /**
   * writes out folders
   *
   * @param folders folders to output to destination, duplicates will be duplicated,
   * @return the result of the attempt
   */
  JobResult folderOutput(Collection<Collection<String>> folders);

  /**
   * writes out pds
   *
   * @param pdsSql pds data to a destination, duplicates will be duplicated
   * @return the result of an attempt
   */
  JobResult writePDSs(Collection<PdsSql> pdsSql);

  /**
   * writes out vds
   *
   * @param vdsSql vds data to a destination, duplicates will be duplicated
   * @param vdsReferenceInfo information related to vds creation
   * @return the result of an attempt
   */
  JobResult writeVDSs(Collection<VdsSql> vdsSql, Collection<VdsReference> vdsReferenceInfo);

  /**
   * creates sources
   *
   * @param sources sources to be created at the destination, duplicates will be duplicated
   * @param defaultCtasFormat the default ctas format to set on the sources.
   * @return the result of the attempt
   */
  JobResult sourceOutput(Collection<String> sources, Optional<String> defaultCtasFormat);
}
