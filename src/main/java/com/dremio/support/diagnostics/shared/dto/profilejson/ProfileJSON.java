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
public class ProfileJSON {
  private String plan;

  private ResourceSchedulingProfile resourceSchedulingProfile;

  private long numPlanCacheUsed;

  private long end;

  private String query;

  private long planningEnd;

  private String jsonPlan;

  private int state;

  private long finishedFragments;

  private List<FragmentProfile> fragmentProfile;

  private AccelerationProfile accelerationProfile;

  private Foreman foreman;

  private String serializedPlan;

  private List<ProfileState> stateList;

  private String dremioVersion;

  private long start;

  private ClientInfo clientInfo;

  private String user;

  private List<PlanPhases> planPhases;

  private String nonDefaultOptionsJSON;

  private List<DatasetProfile> datasetProfile;

  private OperatorTypeMetricsMap operatorTypeMetricsMap;

  private long totalFragments;

  private long planningStart;

  private List<NodeProfile> nodeProfile;

  private long commandPoolWaitMillis;

  private Id id;

  public void setPlan(final String plan) {
    this.plan = plan;
  }

  public String getPlan() {
    return this.plan;
  }

  public void setResourceSchedulingProfile(
      final ResourceSchedulingProfile resourceSchedulingProfile) {
    this.resourceSchedulingProfile = resourceSchedulingProfile;
  }

  public ResourceSchedulingProfile getResourceSchedulingProfile() {
    return this.resourceSchedulingProfile;
  }

  public void setNumPlanCacheUsed(final long numPlanCacheUsed) {
    this.numPlanCacheUsed = numPlanCacheUsed;
  }

  public long getNumPlanCacheUsed() {
    return this.numPlanCacheUsed;
  }

  public void setEnd(final long end) {
    this.end = end;
  }

  public long getEnd() {
    return this.end;
  }

  public void setQuery(final String query) {
    this.query = query;
  }

  public String getQuery() {
    return this.query;
  }

  public void setPlanningEnd(final long planningEnd) {
    this.planningEnd = planningEnd;
  }

  public long getPlanningEnd() {
    return this.planningEnd;
  }

  public void setJsonPlan(final String jsonPlan) {
    this.jsonPlan = jsonPlan;
  }

  public String getJsonPlan() {
    return this.jsonPlan;
  }

  public void setState(final int state) {
    this.state = state;
  }

  public int getState() {
    return this.state;
  }

  public void setFinishedFragments(final long finishedFragments) {
    this.finishedFragments = finishedFragments;
  }

  public long getFinishedFragments() {
    return this.finishedFragments;
  }

  public void setFragmentProfile(final List<FragmentProfile> fragmentProfile) {
    this.fragmentProfile = fragmentProfile;
  }

  public List<FragmentProfile> getFragmentProfile() {
    return this.fragmentProfile;
  }

  public void setAccelerationProfile(final AccelerationProfile accelerationProfile) {
    this.accelerationProfile = accelerationProfile;
  }

  public AccelerationProfile getAccelerationProfile() {
    return this.accelerationProfile;
  }

  public void setForeman(final Foreman foreman) {
    this.foreman = foreman;
  }

  public Foreman getForeman() {
    return this.foreman;
  }

  public void setSerializedPlan(final String serializedPlan) {
    this.serializedPlan = serializedPlan;
  }

  public String getSerializedPlan() {
    return this.serializedPlan;
  }

  public void setStateList(final List<ProfileState> stateList) {
    this.stateList = stateList;
  }

  public List<ProfileState> getStateList() {
    return this.stateList;
  }

  public void setDremioVersion(final String dremioVersion) {
    this.dremioVersion = dremioVersion;
  }

  public String getDremioVersion() {
    return this.dremioVersion;
  }

  public void setStart(final long start) {
    this.start = start;
  }

  public long getStart() {
    return this.start;
  }

  public void setClientInfo(final ClientInfo clientInfo) {
    this.clientInfo = clientInfo;
  }

  public ClientInfo getClientInfo() {
    return this.clientInfo;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  public String getUser() {
    return this.user;
  }

  public void setPlanPhases(final List<PlanPhases> planPhases) {
    this.planPhases = planPhases;
  }

  public List<PlanPhases> getPlanPhases() {
    return this.planPhases;
  }

  public void setNonDefaultOptionsJSON(final String nonDefaultOptionsJSON) {
    this.nonDefaultOptionsJSON = nonDefaultOptionsJSON;
  }

  public String getNonDefaultOptionsJSON() {
    return this.nonDefaultOptionsJSON;
  }

  public void setDatasetProfile(final List<DatasetProfile> datasetProfile) {
    this.datasetProfile = datasetProfile;
  }

  public List<DatasetProfile> getDatasetProfile() {
    return this.datasetProfile;
  }

  public void setOperatorTypeMetricsMap(final OperatorTypeMetricsMap operatorTypeMetricsMap) {
    this.operatorTypeMetricsMap = operatorTypeMetricsMap;
  }

  public OperatorTypeMetricsMap getOperatorTypeMetricsMap() {
    return this.operatorTypeMetricsMap;
  }

  public void setTotalFragments(final long totalFragments) {
    this.totalFragments = totalFragments;
  }

  public long getTotalFragments() {
    return this.totalFragments;
  }

  public void setPlanningStart(final long planningStart) {
    this.planningStart = planningStart;
  }

  public long getPlanningStart() {
    return this.planningStart;
  }

  public void setNodeProfile(final List<NodeProfile> nodeProfile) {
    this.nodeProfile = nodeProfile;
  }

  public List<NodeProfile> getNodeProfile() {
    return this.nodeProfile;
  }

  public void setCommandPoolWaitMillis(final long commandPoolWaitMillis) {
    this.commandPoolWaitMillis = commandPoolWaitMillis;
  }

  public long getCommandPoolWaitMillis() {
    return this.commandPoolWaitMillis;
  }

  public void setId(final Id id) {
    this.id = id;
  }

  public Id getId() {
    return this.id;
  }
}
