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
package com.dremio.support.diagnostics.shared;

import java.io.IOException;

/** Simple wrapper around System.out.println */
public class ConsoleReporter implements Reporter {

  /**
   * Just a simple wrapper around System.out.println
   *
   * @param report report to output to the terminal
   * @throws IOException can happen when generating the report
   */
  @Override
  public void output(Report report) throws IOException {
    System.out.println(report.getText());
  }
}
