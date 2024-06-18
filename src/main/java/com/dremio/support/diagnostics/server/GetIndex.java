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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

// GetIndex reads the index.html resource and returns it as a string
public class GetIndex implements Handler {
  private static final Logger logger = Logger.getLogger(GetIndex.class.getName());
  private final String indexHtmlText;

  /**
   * Will read the index.html resource and store it's value in a string field to
   * be severed up by
   * the handle method
   *
   * @throws IOException is generated when the resource for index.html is not
   *                     found
   */
  public GetIndex() throws IOException {
    StringBuilder builder = new StringBuilder();
    try (InputStream input =
        DQDWebServer.class.getResourceAsStream(
            "/com/dremio/support/diagnostics/server/index.html")) {
      if (input == null) {
        logger.severe("unable to server index.html because it is missing");
        throw new InvalidParameterException("missing index.html");
      }
      try (InputStreamReader reader = new InputStreamReader(input, UTF_8)) {
        try (BufferedReader bufReader = new BufferedReader(reader)) {
          bufReader.lines().forEach(line -> builder.append(String.format("%s%n", line)));
        }
      }
    }
    this.indexHtmlText = builder.toString();
  }

  @Override
  public void handle(@NotNull Context context) throws Exception {
    context.html(this.indexHtmlText);
  }
}
