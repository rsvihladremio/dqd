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

import java.util.List;
import java.util.Map;

public class LogicalJoin extends ConvertToRel {

  private final String joinType;
  private final String condition;

  public LogicalJoin(List<ConvertToRel> children, Map<String, String> properties) {
    super(LogicalJoin.class.getSimpleName(), children, properties);
    this.joinType = getStringProp("joinType");
    this.condition = getStringProp("condition");
  }

  public String getJoinType() {
    return this.joinType;
  }

  /**
   * the condition of the LogicalJoin used to tie the tables together
   *
   * @return the condition as algebra
   */
  public String getCondition() {
    return this.condition;
  }

  @Override
  public String toString() {
    return "LogicalJoin [joinType=" + joinType + ", condition=" + condition + "]";
  }
}
