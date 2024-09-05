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

import com.dremio.support.diagnostics.profilejson.singlefile.SingleProfileJsonHtmlReport;
import com.dremio.support.diagnostics.repro.ArgSetup;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.UsageEntry;
import com.dremio.support.diagnostics.shared.UsageLogger;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

public class PostProfile implements Handler {

  private static final Logger logger = Logger.getLogger(PostProfile.class.getName());
  private UsageLogger usageLogger;

  public PostProfile(UsageLogger usageLogger) {
    this.usageLogger = usageLogger;
  }

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    var start = Instant.now();
    var uploadedFiles = ctx.uploadedFiles();
    if (uploadedFiles.size() != 1) {
      throw new IllegalArgumentException(
          "must upload  only one file but had %d".formatted(uploadedFiles.size()));
    }
    var file = uploadedFiles.get(0);

    try (InputStream is = file.content()) {
      ProfileProvider profileProvider =
          ArgSetup.getProfileProvider(new PathAndStream(Paths.get(file.filename()), is));
      ProfileJSON p = profileProvider.getProfile();

      // now we just always enable this
      final boolean showPlanDetails = true;
      final boolean showConvertToRel = true;
      final String text =
          new SingleProfileJsonHtmlReport(showPlanDetails, showConvertToRel, p).getText();
      ctx.html(text);
      return;
    } catch (Exception e) {
      logger.log(Level.SEVERE, "error reading uploaded file", e);
      ctx.html("<html><body>" + e.getMessage() + "</body>");
    } finally {
      logger.info("profile analysis report generated");
      var end = Instant.now();
      usageLogger.LogUsage(
          new UsageEntry(
              start.getEpochSecond(), end.getEpochSecond(), "profile-json-detailed", ctx.ip()));
    }
  }
}
