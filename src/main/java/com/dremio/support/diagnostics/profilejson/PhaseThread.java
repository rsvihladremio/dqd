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

import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import java.util.List;

public class PhaseThread {
  private int phaseId;
  private long threadId;
  private long runDuration;
  private long blockedDuration;
  private long blockedOnDownstreamDuration;
  private long blockedOnUpstreamDuration;
  private long blockedOnSharedResourceDuration;
  private long sleepingDuration;
  private long totalTimeMillis;
  private double peakMemory;
  private List<OperatorProfile> operators;
  private long endTime;
  private long startTime;

  public long getBlockedDuration() {
    return blockedDuration;
  }

  public void setBlockedDuration(final long blockedDuration) {
    this.blockedDuration = blockedDuration;
  }

  public long getThreadId() {
    return this.threadId;
  }

  public void setThreadId(final long threadId) {
    this.threadId = threadId;
  }

  public int getPhaseId() {
    return phaseId;
  }

  public void setPhaseId(final int phaseId) {
    this.phaseId = phaseId;
  }

  public long getRunDuration() {
    return runDuration;
  }

  public void setRunDuration(final long runDuration) {
    this.runDuration = runDuration;
  }

  public long getBlockedOnDownstreamDuration() {
    return blockedOnDownstreamDuration;
  }

  public void setBlockedOnDownstreamDuration(final long blockedOnDownstreamDuration) {
    this.blockedOnDownstreamDuration = blockedOnDownstreamDuration;
  }

  public long getBlockedOnUpstreamDuration() {
    return blockedOnUpstreamDuration;
  }

  public void setBlockedOnUpstreamDuration(final long blockedOnUpstreamDuration) {
    this.blockedOnUpstreamDuration = blockedOnUpstreamDuration;
  }

  public long getBlockedOnSharedResourceDuration() {
    return blockedOnSharedResourceDuration;
  }

  public void setBlockedOnSharedResourceDuration(final long blockedOnSharedResourceDuration) {
    this.blockedOnSharedResourceDuration = blockedOnSharedResourceDuration;
  }

  public long getSleepingDuration() {
    return sleepingDuration;
  }

  public void setSleepingDuration(final long sleepingDuration) {
    this.sleepingDuration = sleepingDuration;
  }

  public double getPeakMemory() {
    return peakMemory;
  }

  public void setPeakMemory(final double peakMemory) {
    this.peakMemory = peakMemory;
  }

  public long getTotalTimeMillis() {
    return totalTimeMillis;
  }

  public void setTotalTimeMillis(final long totalTimeMillis) {
    this.totalTimeMillis = totalTimeMillis;
  }

  public void setOperators(final List<OperatorProfile> operators) {
    this.operators = operators;
  }

  public List<OperatorProfile> getOperators() {
    return operators;
  }

  public void setEndTime(final long endTime) {
    this.endTime = endTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  public long getStartTime() {
    return startTime;
  }
}
