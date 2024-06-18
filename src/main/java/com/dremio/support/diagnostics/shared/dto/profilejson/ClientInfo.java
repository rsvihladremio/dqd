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
package com.dremio.support.diagnostics.shared.dto.profilejson;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClientInfo {

  private String application;

  private long buildNumber;

  private String versionQualifier;

  private String name;

  private String version;

  private long majorVersion;

  private long minorVersion;

  private long patchVersion;

  public void setApplication(final String application) {
    this.application = application;
  }

  public String getApplication() {
    return this.application;
  }

  public void setBuildNumber(final long buildNumber) {
    this.buildNumber = buildNumber;
  }

  public long getBuildNumber() {
    return this.buildNumber;
  }

  public void setVersionQualifier(final String versionQualifier) {
    this.versionQualifier = versionQualifier;
  }

  public String getVersionQualifier() {
    return this.versionQualifier;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public void setVersion(final String version) {
    this.version = version;
  }

  public String getVersion() {
    return this.version;
  }

  public void setMajorVersion(final long majorVersion) {
    this.majorVersion = majorVersion;
  }

  public long getMajorVersion() {
    return this.majorVersion;
  }

  public void setMinorVersion(final long minorVersion) {
    this.minorVersion = minorVersion;
  }

  public long getMinorVersion() {
    return this.minorVersion;
  }

  public void setPatchVersion(final long patchVersion) {
    this.patchVersion = patchVersion;
  }

  public long getPatchVersion() {
    return this.patchVersion;
  }
}
