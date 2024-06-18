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
public class Foreman {

  private long userPort;

  private Roles roles;

  private long availableCores;

  private String nodeTag;

  private String dremioVersion;

  private String address;

  private long fabricPort;

  private double startTime;

  private double maxDirectMemory;

  private long conduitPort;

  public void setUserPort(final long userPort) {
    this.userPort = userPort;
  }

  public long getUserPort() {
    return this.userPort;
  }

  public void setRoles(final Roles roles) {
    this.roles = roles;
  }

  public Roles getRoles() {
    return this.roles;
  }

  public void setAvailableCores(final long availableCores) {
    this.availableCores = availableCores;
  }

  public long getAvailableCores() {
    return this.availableCores;
  }

  public void setNodeTag(final String nodeTag) {
    this.nodeTag = nodeTag;
  }

  public String getNodeTag() {
    return this.nodeTag;
  }

  public void setDremioVersion(final String dremioVersion) {
    this.dremioVersion = dremioVersion;
  }

  public String getDremioVersion() {
    return this.dremioVersion;
  }

  public void setAddress(final String address) {
    this.address = address;
  }

  public String getAddress() {
    return this.address;
  }

  public void setFabricPort(final long fabricPort) {
    this.fabricPort = fabricPort;
  }

  public long getFabricPort() {
    return this.fabricPort;
  }

  public void setStartTime(final double startTime) {
    this.startTime = startTime;
  }

  public double getStartTime() {
    return this.startTime;
  }

  public void setMaxDirectMemory(final double maxDirectMemory) {
    this.maxDirectMemory = maxDirectMemory;
  }

  public double getMaxDirectMemory() {
    return this.maxDirectMemory;
  }

  public void setConduitPort(final long conduitPort) {
    this.conduitPort = conduitPort;
  }

  public long getConduitPort() {
    return this.conduitPort;
  }
}
