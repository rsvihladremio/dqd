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
public class AccelerationProfile {

  private List<Object> normalizedQueryPlans;

  private String accelerationDetails;

  private boolean accelerated;

  private long numSubstitutions;

  private long millisTakenGettingMaterializations;

  private long millisTakenNormalizing;

  private long millisTakenSubstituting;

  private List<Object> layoutProfiles;

  public void setNormalizedQueryPlans(final List<Object> normalizedQueryPlans) {
    this.normalizedQueryPlans = normalizedQueryPlans;
  }

  public List<Object> getNormalizedQueryPlans() {
    return this.normalizedQueryPlans;
  }

  public void setAccelerationDetails(final String accelerationDetails) {
    this.accelerationDetails = accelerationDetails;
  }

  public String getAccelerationDetails() {
    return this.accelerationDetails;
  }

  public void setAccelerated(final boolean accelerated) {
    this.accelerated = accelerated;
  }

  public boolean getAccelerated() {
    return this.accelerated;
  }

  public void setNumSubstitutions(final long numSubstitutions) {
    this.numSubstitutions = numSubstitutions;
  }

  public long getNumSubstitutions() {
    return this.numSubstitutions;
  }

  public void setMillisTakenGettingMaterializations(final long millisTakenGettingMaterializations) {
    this.millisTakenGettingMaterializations = millisTakenGettingMaterializations;
  }

  public long getMillisTakenGettingMaterializations() {
    return this.millisTakenGettingMaterializations;
  }

  public void setMillisTakenNormalizing(final long millisTakenNormalizing) {
    this.millisTakenNormalizing = millisTakenNormalizing;
  }

  public long getMillisTakenNormalizing() {
    return this.millisTakenNormalizing;
  }

  public void setMillisTakenSubstituting(final long millisTakenSubstituting) {
    this.millisTakenSubstituting = millisTakenSubstituting;
  }

  public long getMillisTakenSubstituting() {
    return this.millisTakenSubstituting;
  }

  public void setLayoutProfiles(final List<Object> layoutProfiles) {
    this.layoutProfiles = layoutProfiles;
  }

  public List<Object> getLayoutProfiles() {
    return this.layoutProfiles;
  }
}
