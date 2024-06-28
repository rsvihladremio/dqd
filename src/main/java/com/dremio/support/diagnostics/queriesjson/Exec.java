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
package com.dremio.support.diagnostics.queriesjson;

import com.dremio.support.diagnostics.shared.*;
import java.io.*;

/** entry point for the queries-json command */
public class Exec {

  /**
   * starts the queries-json command (has the following properties) - is
   * multi-threaded -
   *
   * @param files filepath, directory, or comma separated list of files to read
   */
  public void run(final QueriesJsonHtmlReport report, final Reporter reporter) throws IOException {
    reporter.output(report);
  }
}
