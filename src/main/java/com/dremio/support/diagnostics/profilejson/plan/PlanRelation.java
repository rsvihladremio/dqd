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
package com.dremio.support.diagnostics.profilejson.plan;

import java.util.List;
import java.util.Map;

public class PlanRelation {
  private final String name;
  private final String op;
  private final Map<String, Object> values;
  private final List<PlanRelation> upstream;
  private final List<PlanRelation> downstream;
  // private final ConvertToRel parent;
  private final double rowCount;
  // private final Map<String, ColumnType> rowType;
  private final CumulativeCost cumulativeCost;

  public PlanRelation(
      final String name,
      final String op,
      final Map<String, Object> values,
      final List<PlanRelation> downstream,
      final List<PlanRelation> upstream,
      final double rowCount,
      // final Map<String, ColumnType> rowType,
      final CumulativeCost cumulativeCost) {
    this.name = name;
    this.op = op;
    this.values = values;
    this.downstream = downstream;
    this.upstream = upstream;
    // this.parent = parent;
    this.rowCount = rowCount;
    // this.rowType = rowType;
    this.cumulativeCost = cumulativeCost;
  }

  public String getOp() {
    return op;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  public List<PlanRelation> getUpstream() {
    return upstream;
  }

  public List<PlanRelation> getDownstream() {
    return downstream;
  }

  // public ConvertToRel getParent() {
  // return parent;
  // }

  public double getRowCount() {
    return rowCount;
  }

  // public Map<String, ColumnType> getRowType() {
  // return rowType;
  // }

  public CumulativeCost getCumulativeCost() {
    return cumulativeCost;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "PlanRelation [name="
        + name
        + ", op="
        + op
        + ", values="
        + values
        + ", rowCount="
        + rowCount
        + ", cumulativeCost="
        + cumulativeCost
        + "]";
  }
}
