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
package com.dremio.support.diagnostics.queriesjson;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Query {
  // private Object materializationFor;
  private String queryId;

  // private String context;
  private String queryText;

  private long start;
  private long finish;
  private String outcome;
  private String outcomeReason;
  private String username;

  // private long inputRecords;
  // private long inputBytes;
  // private long outputRecords;
  // private long outputBytes;
  // private String requestType;
  private String queryType;
  // private Object[] parentsList;
  // private boolean accelerated;
  // private Object[] reflectionRelationships;
  private float queryCost;
  private String queueName;
  private long poolWaitTime; // in milliseconds
  // https//github.com/dremio/dremio/commit/d35c86fd9f27a75a36da88847e597ae519cf9ef9#diff-ccfa6fd859ea1c6d0a88df31e12a83ab24a2fc45717f2175c0a5b1e0d045e1bdR78
  private long pendingTime;
  private long metadataRetrievalTime;
  private long planningTime;
  // private long engineStartTime;
  private long queuedTime;
  // private long executionPlanningTime;
  private long startingTime;
  private long runningTime;
  private String engineName;
  private long attemptCount;
  // private long submitted;
  private long metadataRetrieval;
  private long planningStart;
  private long queryEnqueued;
  // private long engineStart;
  // private long executionPlanningStart;
  // private long executionStart;
  // private Object[] scannedDatasets;
  // private Object[] executionNodes;
  // private long executionCpuTimeNs;
  // private long setupTimeNs;
  // private long waitTimeNs;
  private long memoryAllocated;

  public Query() {}

  public String getOutcome() {
    return outcome;
  }

  public void setOutcome(String outcome) {
    this.outcome = outcome;
  }

  public String getOutcomeReason() {
    return outcomeReason;
  }

  public void setOutcomeReason(String outcomeReason) {
    this.outcomeReason = outcomeReason;
  }

  public String getQueryText() {
    return queryText;
  }

  public void setQueryText(String queryText) {
    this.queryText = queryText;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getFinish() {
    return finish;
  }

  public void setFinish(long finish) {
    this.finish = finish;
  }

  public String getQueryType() {
    return queryType;
  }

  public void setQueryType(String queryType) {
    this.queryType = queryType;
  }

  public float getQueryCost() {
    return queryCost;
  }

  public void setQueryCost(float queryCost) {
    this.queryCost = queryCost;
  }

  public String getQueueName() {
    if (queueName == null || queueName.isEmpty()) {
      return "Default";
    }
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public long getPoolWaitTime() {
    return poolWaitTime;
  }

  public void setPoolWaitTime(long poolWaitTime) {
    this.poolWaitTime = poolWaitTime;
  }

  public long getPendingTime() {
    return pendingTime;
  }

  public void setPendingTime(long pendingTime) {
    this.pendingTime = pendingTime;
  }

  public long getPlanningTime() {
    return planningTime;
  }

  public void setPlanningTime(long planningTime) {
    this.planningTime = planningTime;
  }

  public long getQueuedTime() {
    return queuedTime;
  }

  public void setQueuedTime(long queuedTime) {
    this.queuedTime = queuedTime;
  }

  public long getStartingTime() {
    return startingTime;
  }

  public void setStartingTime(long startingTime) {
    this.startingTime = startingTime;
  }

  public long getRunningTime() {
    return runningTime;
  }

  public void setRunningTime(long runningTime) {
    this.runningTime = runningTime;
  }

  public String getEngineName() {
    if (engineName == null || engineName.isEmpty()) {
      return "Default";
    }
    return engineName;
  }

  public void setEngineName(String engineName) {
    this.engineName = engineName;
  }

  public long getAttemptCount() {
    return attemptCount;
  }

  public void setAttemptCount(long attemptCount) {
    this.attemptCount = attemptCount;
  }

  /**
   * do not use this as depending on the version of the profile this will either
   * be time since epoch
   * or the duration of the metadata retrieval
   *
   * @return depending on the version of the profile this will be either the time
   *         when the metadata
   *         retrieval occurred
   */
  @Deprecated
  public long getMetadataRetrieval() {
    return metadataRetrieval;
  }

  public void setMetadataRetrieval(long metadataRetrieval) {
    this.metadataRetrieval = metadataRetrieval;
  }

  public long getPlanningStart() {
    return planningStart;
  }

  public void setPlanningStart(long planningStart) {
    this.planningStart = planningStart;
  }

  public long getQueryEnqueued() {
    return queryEnqueued;
  }

  public void setQueryEnqueued(long queryEnqueued) {
    this.queryEnqueued = queryEnqueued;
  }

  public long getMemoryAllocated() {
    return memoryAllocated;
  }

  public void setMemoryAllocated(long memoryAllocated) {
    this.memoryAllocated = memoryAllocated;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Query)) return false;
    Query query = (Query) o;
    return start == query.start
        && finish == query.finish
        && Float.compare(query.queryCost, queryCost) == 0
        && poolWaitTime == query.poolWaitTime
        && pendingTime == query.pendingTime
        && planningTime == query.planningTime
        && queuedTime == query.queuedTime
        && startingTime == query.startingTime
        && runningTime == query.runningTime
        && attemptCount == query.attemptCount
        && metadataRetrieval == query.metadataRetrieval
        && planningStart == query.planningStart
        && queryEnqueued == query.queryEnqueued
        && memoryAllocated == query.memoryAllocated
        && Objects.equals(username, query.username)
        && Objects.equals(queryId, query.queryId)
        && Objects.equals(queryText, query.queryText)
        && Objects.equals(queryType, query.queryType)
        && Objects.equals(queueName, query.queueName)
        && Objects.equals(engineName, query.engineName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        queryText,
        start,
        finish,
        queryType,
        queryCost,
        queueName,
        poolWaitTime,
        pendingTime,
        planningTime,
        queuedTime,
        startingTime,
        runningTime,
        engineName,
        attemptCount,
        metadataRetrieval,
        planningStart,
        queryEnqueued,
        memoryAllocated,
        username,
        queryId);
  }

  /**
   * do not use this as depending on the version of the profile this will either
   * be time since epoch
   * or the duration of the metadata retrieval
   *
   * @return depending on the version of the profile this will be either the time
   *         when the metadata
   *         retrieval occurred
   */
  @Deprecated
  public long getMetadataRetrievalTime() {
    return metadataRetrievalTime;
  }

  public void setMetadataRetrievalTime(long metadataRetrievalTime) {
    this.metadataRetrievalTime = metadataRetrievalTime;
  }

  private final long epochSince2017 = 1500000000000L;

  public long getNormalizedMetadataRetrieval() {
    if (metadataRetrieval > epochSince2017) {
      return metadataRetrievalTime;
    }
    return metadataRetrieval;
  }
}
