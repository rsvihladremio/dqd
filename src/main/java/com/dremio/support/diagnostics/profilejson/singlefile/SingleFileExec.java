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

import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.logging.Logger;
import java.nio.file.Files;
import java.nio.file.Path;

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
  public final void run() throws IOException {
    final ProfileJSON parsed = this.provider.getProfile();
    if (parsed == null) {
      throw new InvalidParameterException(
          String.format("file '%s' not parse-able", this.provider.getFilePath()));
    }
    long epoch = Instant.now().toEpochMilli();
    var reporter = new SingleProfileJsonHtmlReport(true, true, parsed);
    var path = String.format("profile%d.html", epoch);
    Files.write(Path.of(path), reporter.getText().getBytes());
    logger.info("report written to '%s'".formatted(path));
  }
}
