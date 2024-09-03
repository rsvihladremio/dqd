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

import com.dremio.support.diagnostics.shared.DQDVersion;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.jetbrains.annotations.NotNull;

// GetIndex reads the index.html resource and returns it as a string
public class GetAbout implements Handler {

  private final String version;

  public GetAbout() {
    this.version = DQDVersion.getVersion();
  }

  @Override
  public void handle(@NotNull Context ctx) throws Exception {
    ctx.result("{\"version\": \"%s\"}".formatted(version));
  }
}
