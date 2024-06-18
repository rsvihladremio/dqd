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
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MinorFragmentProfile {

  private long lastUpdate;

  private long runDuration;

  private long numRuns;

  private int state;

  private long minorFragmentId;

  private List<OperatorProfile> operatorProfile;

  private long blockedDuration;

  private long blockedOnDownstreamDuration;

  private List<PerResourceBlockedDuration> perResourceBlockedDuration;

  private long startTime;

  private long memoryUsed;

  private Endpoint endpoint;

  private long blockedOnSharedResourceDuration;

  private long endTime;

  private double maxMemoryUsed;

  private double firstRun;

  private long finishDuration;

  private long blockedOnUpstreamDuration;

  private double lastProgress;

  private long sleepingDuration;

  private long setupDuration;

  public void setLastUpdate(final long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  public long getLastUpdate() {
    return this.lastUpdate;
  }

  public void setRunDuration(final long runDuration) {
    this.runDuration = runDuration;
  }

  public long getRunDuration() {
    return this.runDuration;
  }

  public void setNumRuns(final long numRuns) {
    this.numRuns = numRuns;
  }

  public long getNumRuns() {
    return this.numRuns;
  }

  public void setState(final int state) {
    this.state = state;
  }

  public int getState() {
    return this.state;
  }

  public void setMinorFragmentId(final long minorFragmentId) {
    this.minorFragmentId = minorFragmentId;
  }

  public long getMinorFragmentId() {
    return this.minorFragmentId;
  }

  public void setOperatorProfile(final List<OperatorProfile> operatorProfile) {
    this.operatorProfile = operatorProfile;
  }

  public List<OperatorProfile> getOperatorProfile() {
    return this.operatorProfile;
  }

  public void setBlockedDuration(final long blockedDuration) {
    this.blockedDuration = blockedDuration;
  }

  public long getBlockedDuration() {
    return this.blockedDuration;
  }

  public void setBlockedOnDownstreamDuration(final long blockedOnDownstreamDuration) {
    this.blockedOnDownstreamDuration = blockedOnDownstreamDuration;
  }

  public long getBlockedOnDownstreamDuration() {
    return this.blockedOnDownstreamDuration;
  }

  public void setPerResourceBlockedDuration(
      final List<PerResourceBlockedDuration> perResourceBlockedDuration) {
    this.perResourceBlockedDuration = perResourceBlockedDuration;
  }

  public List<PerResourceBlockedDuration> getPerResourceBlockedDuration() {
    return this.perResourceBlockedDuration;
  }

  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public void setMemoryUsed(final long memoryUsed) {
    this.memoryUsed = memoryUsed;
  }

  public long getMemoryUsed() {
    return this.memoryUsed;
  }

  public void setEndpoint(final Endpoint endpoint) {
    this.endpoint = endpoint;
  }

  public Endpoint getEndpoint() {
    return this.endpoint;
  }

  public void setBlockedOnSharedResourceDuration(final long blockedOnSharedResourceDuration) {
    this.blockedOnSharedResourceDuration = blockedOnSharedResourceDuration;
  }

  public long getBlockedOnSharedResourceDuration() {
    return this.blockedOnSharedResourceDuration;
  }

  public void setEndTime(final long endTime) {
    this.endTime = endTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public void setMaxMemoryUsed(final double maxMemoryUsed) {
    this.maxMemoryUsed = maxMemoryUsed;
  }

  public double getMaxMemoryUsed() {
    return this.maxMemoryUsed;
  }

  public void setFirstRun(final double firstRun) {
    this.firstRun = firstRun;
  }

  public double getFirstRun() {
    return this.firstRun;
  }

  public void setFinishDuration(final long finishDuration) {
    this.finishDuration = finishDuration;
  }

  public long getFinishDuration() {
    return this.finishDuration;
  }

  public void setBlockedOnUpstreamDuration(final long blockedOnUpstreamDuration) {
    this.blockedOnUpstreamDuration = blockedOnUpstreamDuration;
  }

  public long getBlockedOnUpstreamDuration() {
    return this.blockedOnUpstreamDuration;
  }

  public void setLastProgress(final double lastProgress) {
    this.lastProgress = lastProgress;
  }

  public double getLastProgress() {
    return this.lastProgress;
  }

  public void setSleepingDuration(final long sleepingDuration) {
    this.sleepingDuration = sleepingDuration;
  }

  public long getSleepingDuration() {
    return this.sleepingDuration;
  }

  public void setSetupDuration(final long setupDuration) {
    this.setupDuration = setupDuration;
  }

  public long getSetupDuration() {
    return this.setupDuration;
  }
}
