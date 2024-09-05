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

import com.dremio.support.diagnostics.top.TopExec;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

public class PostTop implements Handler {
  private static final Logger logger = Logger.getLogger(PostTop.class.getName());

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    var uploadedFiles = ctx.uploadedFiles();
    if (uploadedFiles.size() != 1) {
      throw new IllegalArgumentException(
          "must upload  only one file but had %d".formatted(uploadedFiles.size()));
    }
    var file = uploadedFiles.get(0);
    try (InputStream is = file.content()) {
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        TopExec.exec(is, baos);
        ctx.html(baos.toString("UTF-8"));
        return;
      } catch (Exception ex) {
        logger.log(Level.SEVERE, "error reading uploaded file", ex);
        ctx.html("<html><body>" + ex.getMessage() + "</body>");
        return;
      }
    }
  }
}
