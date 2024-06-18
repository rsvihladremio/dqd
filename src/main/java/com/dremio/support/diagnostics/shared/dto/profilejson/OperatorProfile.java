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
public class OperatorProfile {

  private long processNanos;

  private List<Metric> metric;

  private long waitNanos;

  private long operatorSubtype;

  private List<InputProfile> inputProfile;

  private long operatorId;

  private int operatorType;

  private long setupNanos;

  private long peakLocalMemoryAllocated;

  public void setProcessNanos(final long processNanos) {
    this.processNanos = processNanos;
  }

  public long getProcessNanos() {
    return this.processNanos;
  }

  public void setMetric(final List<Metric> metric) {
    this.metric = metric;
  }

  public List<Metric> getMetric() {
    return this.metric;
  }

  public void setWaitNanos(final long waitNanos) {
    this.waitNanos = waitNanos;
  }

  public long getWaitNanos() {
    return this.waitNanos;
  }

  public void setOperatorSubtype(final long operatorSubtype) {
    this.operatorSubtype = operatorSubtype;
  }

  public long getOperatorSubtype() {
    return this.operatorSubtype;
  }

  public void setInputProfile(final List<InputProfile> inputProfile) {
    this.inputProfile = inputProfile;
  }

  public List<InputProfile> getInputProfile() {
    return this.inputProfile;
  }

  public void setOperatorId(final long operatorId) {
    this.operatorId = operatorId;
  }

  public long getOperatorId() {
    return this.operatorId;
  }

  public void setOperatorType(final int operatorType) {
    this.operatorType = operatorType;
  }

  public int getOperatorType() {
    return this.operatorType;
  }

  public void setSetupNanos(final long setupNanos) {
    this.setupNanos = setupNanos;
  }

  public long getSetupNanos() {
    return this.setupNanos;
  }

  public void setPeakLocalMemoryAllocated(final long peakLocalMemoryAllocated) {
    this.peakLocalMemoryAllocated = peakLocalMemoryAllocated;
  }

  public long getPeakLocalMemoryAllocated() {
    return this.peakLocalMemoryAllocated;
  }
}
