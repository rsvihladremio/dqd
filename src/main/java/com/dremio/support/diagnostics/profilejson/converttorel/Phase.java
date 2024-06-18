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
package com.dremio.support.diagnostics.profilejson.converttorel;

import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import java.util.List;

public class Phase {

  private int id;
  private long runDuration;
  private long blockedDuration;
  private long blockedOnDownstreamDuration;
  private long blockedOnUpstreamDuration;
  private long blockedOnSharedResourceDuration;
  private double peakMemory;
  private List<OperatorProfile> operators;

  public List<OperatorProfile> getOperators() {
    return operators;
  }

  public void setOperators(final List<OperatorProfile> operators) {
    this.operators = operators;
  }

  public long getBlockedDuration() {
    return blockedDuration;
  }

  public void setBlockedDuration(final long blockedDuration) {
    this.blockedDuration = blockedDuration;
  }

  public int getId() {
    return id;
  }

  public void setId(final int id) {
    this.id = id;
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

  public double getPeakMemory() {
    return peakMemory;
  }

  public void setPeakMemory(final double peakMemory) {
    this.peakMemory = peakMemory;
  }
}
