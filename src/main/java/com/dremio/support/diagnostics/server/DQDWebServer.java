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

import com.dremio.support.diagnostics.shared.UsageLogger;
import com.dremio.support.diagnostics.simple.ProfileJSONSimplified;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Starts a Javalin web service that has provides several endpoints that
 * generate html reports there
 * is also a web ui that allows end users to generate reports from the URL, no
 * data is stored on
 * disk and all data is parsed in ram and generated in ram, this is to minimize
 * the security issues.
 */
public class DQDWebServer {

  private static final Logger LOGGER = Logger.getLogger(DQDWebServer.class.getName());
  private final Handler getIndex;
  private final Handler postProfile;
  private final Handler postProfiles;
  private final Handler postReproduction;
  private final Handler postQueriesJson;
  private final Handler postSimpleProfile;
  private final Handler getAbout;

  /**
   * Starts a web server
   *
   * @param usageLogger
   * @throws IOException occurs when we are unable to read the index.html file
   */
  public DQDWebServer(final UsageLogger usageLogger)
      throws IOException {
    this(
        new GetIndex(),
        new PostProfile(usageLogger),
        new PostProfiles(usageLogger),
        new PostReproduction(usageLogger),
        new PostQueriesJson(usageLogger),
        new ProfileJSONSimplified.ProfileHTTPEndpoint(usageLogger),
        new GetAbout());
  }

  /**
   * Starts a web server that wires up the specified handlers
   *
   * @param getIndex          wired up to the / url with a GET action
   * @param postProfile       wired up to /profile with a POST action
   * @param postProfiles      wired up to /profiles with a POST action
   * @param postReproduction  wired up to /reproduction with a POST action
   * @param postQueriesJson   wired up to /queriesjson with a POST action
   * @param postSimpleProfile wired up to /simple-profile with a POST action
   * @param getAbout          wired up to /about.json with a GET action
   */
  public DQDWebServer(
      final Handler getIndex,
      final Handler postProfile,
      final Handler postProfiles,
      final Handler postReproduction,
      final Handler postQueriesJson,
      final Handler postSimpleProfile,
      final Handler getAbout) {
    this.getIndex = getIndex;
    this.postProfile = postProfile;
    this.postProfiles = postProfiles;
    this.postReproduction = postReproduction;
    this.postQueriesJson = postQueriesJson;
    this.postSimpleProfile = postSimpleProfile;
    this.getAbout = getAbout;
  }

  /**
   * maps the routes, note there is no default route or specific route for errors.
   * We may need to
   * improve this over time.
   *
   */
  public void launch(int port) {
    try (var app =
        Javalin.create(
                config -> {
                  config.http.maxRequestSize = 10 * 1000000; // 10mb
                })
            .start(port)) {
      app.get("/", this.getIndex);
      app.post("/profile", this.postProfile);
      app.post("/profiles", this.postProfiles);
      app.post("/queriesjson", this.postQueriesJson);
      app.post("/reproduction", this.postReproduction);
      app.post("/simple-profile", this.postSimpleProfile);
      app.get("/about.json", this.getAbout);
      Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
      try {
        Thread.currentThread().join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * preferred method of launching the service
   *
   * @param port                port that the web service runs on
   *
   * @throws Exception can throw thread exceptions and various exceptions that are spawned by javalin
   */
  public static void start(final Integer port) throws Exception {
    final UsageLogger usageLogger;
    LOGGER.warning("logging usage to local logs");
    usageLogger = new LocalUsageLogger();
    new DQDWebServer(usageLogger).launch(port);
  }
}
