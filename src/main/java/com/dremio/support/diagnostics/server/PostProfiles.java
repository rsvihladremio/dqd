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
package com.dremio.support.diagnostics.server;

import com.dremio.support.diagnostics.profilejson.Difference;
import com.dremio.support.diagnostics.profilejson.HtmlProfileComparisonReport;
import com.dremio.support.diagnostics.profilejson.ProfileDifferenceReport;
import com.dremio.support.diagnostics.repro.ArgSetup;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.UsageEntry;
import com.dremio.support.diagnostics.shared.UsageLogger;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

public class PostProfiles implements Handler {
  private static final Logger logger = Logger.getLogger(PostProfiles.class.getName());
  private UsageLogger usageLogger;

  public PostProfiles(UsageLogger usageLogger) {
    this.usageLogger = usageLogger;
  }

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    var start = Instant.now();
    try {
      final var files = ctx.uploadedFiles();
      if (files.isEmpty()) {
        throw new InvalidParameterException("no files");
      }
      final int expectedNumber = 2;
      if (files.size() > expectedNumber) {
        throw new InvalidParameterException("too many files");
      }
      if (files.size() < expectedNumber) {
        throw new InvalidParameterException("not enough files");
      }
      var profile1 = files.get(0);
      var profile2 = files.get(1);
      // hard coded now
      final boolean showPlanComparison = true;
      final boolean showConvertToRel = true;
      logger.warning("parsing profile 1");
      ProfileProvider profile1Provider =
          ArgSetup.getProfileProvider(
              new PathAndStream(Paths.get(profile1.filename()), profile1.content()));
      logger.warning("parsing profile 2");
      ProfileProvider profile2Provider =
          ArgSetup.getProfileProvider(
              new PathAndStream(Paths.get(profile2.filename()), profile2.content()));
      ProfileDifferenceReport differ = new ProfileDifferenceReport();
      // retrieve this ahead of time, you can only parse them once
      ProfileJSON profile1Parsed = profile1Provider.getProfile();
      ProfileJSON profile2Parsed = profile2Provider.getProfile();
      // now generate a list of diffs between the two profiles
      List<Difference> diffs =
          differ.getDifferences(
              profile1.filename(),
              profile2.filename(),
              showPlanComparison,
              profile1Parsed,
              profile2Parsed);
      // now create an html report, which for now nests a text report
      HtmlProfileComparisonReport htmlProfileComparisonReport =
          new HtmlProfileComparisonReport(
              showConvertToRel,
              profile1.filename().toString(),
              profile2.filename().toString(),
              profile1Parsed,
              profile2Parsed,
              diffs);
      String report = htmlProfileComparisonReport.getText();
      ctx.html(report);
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "report unable to read profile.json", ex);
      ctx.html("<html><body>" + ex.getMessage() + "</body>");
    } finally {
      logger.info("profile comparison report generated");
      var end = Instant.now();
      usageLogger.LogUsage(
          new UsageEntry(
              start.getEpochSecond(), end.getEpochSecond(), "profile-json-compare", ctx.ip()));
    }
  }
}
