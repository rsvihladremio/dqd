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
package com.dremio.support.diagnostics.profilejson.singlefile;

import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileSummaryConsoleReport;
import com.dremio.support.diagnostics.shared.ConsoleReporter;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.Report;
import com.dremio.support.diagnostics.shared.Reporter;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.logging.Logger;

/** simplistic first pass analysis of profile.json */
public class SingleFileExec {
  private static final Logger logger = Logger.getLogger(SingleFileExec.class.getName());
  private final ProfileProvider provider;

  /**
   * SingleFileExec does a simple analysis of a profile.json file html
   * file or the console
   *
   * @param provider profile provider that will provide a profile no matter the file format
   */
  public SingleFileExec(final ProfileProvider provider) {
    this.provider = provider;
  }

  /**
   * runs the report, if html is filled out then that is run else the console report is used
   *
   * @param showPlanPhases if this is false a briefer summary will be generated, if true, then a
   *     detailed report of all plan phases will be shown
   * @throws IOException usually when we cannot read the file
   */
  public final void run(final boolean showPlanPhases) throws IOException {
    final ProfileJSON parsed = this.provider.getProfile();
    if (parsed == null) {
      throw new InvalidParameterException(
          String.format("file '%s' not parse-able", this.provider.getFilePath()));
    }
    final Report consoleReport =
        new ProfileSummaryConsoleReport(
            String.valueOf(this.provider.getFilePath()), parsed, showPlanPhases);
    logger.info("console reporter selected");
    final Reporter reporter = new ConsoleReporter();
    reporter.output(consoleReport);
  }
}
