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

import com.dremio.support.diagnostics.repro.ArgSetup;
import com.dremio.support.diagnostics.repro.Exec.ResponseMessage;
import com.dremio.support.diagnostics.repro.SqlOutput;
import com.dremio.support.diagnostics.repro.parse.ColumnDefYaml;
import com.dremio.support.diagnostics.repro.parse.ReproProfileParserImpl;
import com.dremio.support.diagnostics.shared.PathAndStream;
import com.dremio.support.diagnostics.shared.ProfileProvider;
import com.dremio.support.diagnostics.shared.UsageEntry;
import com.dremio.support.diagnostics.shared.UsageLogger;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.zip.ZipOutputStream;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class PostReproduction implements Handler {

  private final UsageLogger usageLogger;

  public PostReproduction(final UsageLogger usageLogger) {
    this.usageLogger = usageLogger;
  }

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    var functionStart = Instant.now();
    try {
      BiFunction<String, String, String> getOrDefault =
          (String k, String defaultValue) -> {
            var v = ctx.formParam(k);
            if (v == null) {
              return defaultValue;
            }
            return v;
          };
      int records = Integer.parseInt(getOrDefault.apply("records", "20"));
      int timeout = Integer.parseInt(getOrDefault.apply("timeout", "60"));
      String nasSourceBaseDir = getOrDefault.apply("nasPath", "");
      String columnDefYamlString = getOrDefault.apply("columnDefYaml", "");
      final ColumnDefYaml columnDef;
      if ("".equals(columnDefYamlString)) {
        columnDef = new ColumnDefYaml();
        columnDef.setTables(new ArrayList<>());
      } else {
        final Yaml yaml = new Yaml(new Constructor(ColumnDefYaml.class, new LoaderOptions()));
        columnDef = yaml.load(columnDefYamlString);
      }
      String rawCtasFormat = getOrDefault.apply("defaultCtasFormat", "");
      final Optional<String> defaultCtasFormat;
      if (rawCtasFormat.isEmpty()) {
        defaultCtasFormat = Optional.empty();
      } else {
        defaultCtasFormat = Optional.of(rawCtasFormat);
      }
      var file = ctx.uploadedFiles().get(0);
      try (InputStream is = file.content()) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
          try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ReproProfileParserImpl profileParser = ArgSetup.getReproProfile(records, columnDef);
            ProfileProvider profileProvider =
                ArgSetup.getProfileProvider(new PathAndStream(Paths.get(file.filename()), is));
            final SqlOutput[] sqlOutput =
                ArgSetup.getSqlOutput(
                    null, null, null, zos, null, null, timeout, nasSourceBaseDir, false);
            com.dremio.support.diagnostics.repro.Exec exec =
                new com.dremio.support.diagnostics.repro.Exec(
                    defaultCtasFormat, profileProvider, profileParser, sqlOutput);
            ResponseMessage response = exec.run();
            if (response.getErrorCode() != 0) {
              ctx.html(
                  "<h2>Error</h2><a href=\"/#reproduction\">go back</a><div"
                      + " style=\"white-space: pre-line;\">"
                      + StringEscapeUtils.unescapeHtml4(response.getErrorText())
                      + "</div>");
              return;
            }
          }
          // need to call this here of the zip output stream will still be open
          // but you cannot call it outside of the ByteArrayOutputStream stream or it
          // will be closed
          ctx.header("Content-Disposition", "attachment; filename=\"repro-scripts.zip\"")
              .header("Content-Type", "application/x-zip-compressed")
              .result(baos.toByteArray());
        }
      }
    } finally {
      String timezone = ctx.formParam("timezone");
      var end = Instant.now();
      usageLogger.LogUsage(
          new UsageEntry(
              functionStart.getEpochSecond(), end.getEpochSecond(), "repro-tool", timezone));
    }
  }
}
