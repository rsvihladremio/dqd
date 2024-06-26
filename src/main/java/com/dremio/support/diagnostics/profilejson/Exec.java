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
package com.dremio.support.diagnostics.profilejson;

import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

public class Exec {

  private static final Logger logger = Logger.getLogger(Exec.class.getName());
  private final Differ differ;
  private final ProfileProvider profile1Provider;
  private final ProfileProvider profile2Provider;

  public Exec(
      final Differ differ,
      final ProfileProvider profile1Provider,
      final ProfileProvider profile2Provider) {
    this.differ = differ;
    this.profile1Provider = profile1Provider;
    this.profile2Provider = profile2Provider;
  }

  public void run() throws IOException {
    final ProfileJSON profile1 = this.profile1Provider.getProfile();
    final ProfileJSON profile2 = this.profile2Provider.getProfile();
    final List<Difference> differences =
        differ.getDifferences(
            this.profile1Provider.getFilePath().toString(),
            this.profile2Provider.getFilePath().toString(),
            true,
            profile1,
            profile2);
    logger.info("console reporter selected");
    var report = new HtmlProfileComparisonReport(
      true,
      this.profile1Provider.getFilePath().toString(),
      this.profile2Provider.getFilePath().toString(),
      profile1,
      profile2,
      differences);
      final long epoch = Instant.now().toEpochMilli();
      String path = String.format("profile-compare-%d.html", epoch);
      Files.write(Path.of(path), report.getText().getBytes());
      System.out.printf("wrote report %s to disk", path.toString());
  }
}
