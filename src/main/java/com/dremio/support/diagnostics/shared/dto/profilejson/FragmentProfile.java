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
public class FragmentProfile {

  private int majorFragmentId;

  private List<MinorFragmentProfile> minorFragmentProfile;

  private List<NodePhaseProfile> nodePhaseProfile;

  public void setMajorFragmentId(final int majorFragmentId) {
    this.majorFragmentId = majorFragmentId;
  }

  public int getMajorFragmentId() {
    return this.majorFragmentId;
  }

  public void setMinorFragmentProfile(final List<MinorFragmentProfile> minorFragmentProfile) {
    this.minorFragmentProfile = minorFragmentProfile;
  }

  public List<MinorFragmentProfile> getMinorFragmentProfile() {
    return this.minorFragmentProfile;
  }

  public void setNodePhaseProfile(final List<NodePhaseProfile> nodePhaseProfile) {
    this.nodePhaseProfile = nodePhaseProfile;
  }

  public List<NodePhaseProfile> getNodePhaseProfile() {
    return this.nodePhaseProfile;
  }
}
