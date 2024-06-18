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
public class ResourceSchedulingProfile {

  private String ruleContent;

  private String ruleName;

  private String ruleAction;

  private SchedulingProperties schedulingProperties;

  private double resourceSchedulingStart;

  private double resourceSchedulingEnd;

  private String queueName;

  private String queueId;

  public void setRuleContent(final String ruleContent) {
    this.ruleContent = ruleContent;
  }

  public String getRuleContent() {
    return this.ruleContent;
  }

  public void setRuleName(final String ruleName) {
    this.ruleName = ruleName;
  }

  public String getRuleName() {
    return this.ruleName;
  }

  public void setRuleAction(final String ruleAction) {
    this.ruleAction = ruleAction;
  }

  public String getRuleAction() {
    return this.ruleAction;
  }

  public void setSchedulingProperties(final SchedulingProperties schedulingProperties) {
    this.schedulingProperties = schedulingProperties;
  }

  public SchedulingProperties getSchedulingProperties() {
    return this.schedulingProperties;
  }

  public void setResourceSchedulingStart(final double resourceSchedulingStart) {
    this.resourceSchedulingStart = resourceSchedulingStart;
  }

  public double getResourceSchedulingStart() {
    return this.resourceSchedulingStart;
  }

  public void setResourceSchedulingEnd(final double resourceSchedulingEnd) {
    this.resourceSchedulingEnd = resourceSchedulingEnd;
  }

  public double getResourceSchedulingEnd() {
    return this.resourceSchedulingEnd;
  }

  public void setQueueName(final String queueName) {
    this.queueName = queueName;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public void setQueueId(final String queueId) {
    this.queueId = queueId;
  }

  public String getQueueId() {
    return this.queueId;
  }
}
