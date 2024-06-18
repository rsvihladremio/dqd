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

import java.util.Objects;

public class OperatorMetric {

  private String metricName;

  private long metricId;

  private Long longValue;

  @Override
  public String toString() {
    return "metricName='" + metricName + "\n  metricId=" + metricId + "\n  longValue=" + longValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OperatorMetric)) return false;
    OperatorMetric that = (OperatorMetric) o;
    return metricId == that.metricId
        && Objects.equals(metricName, that.metricName)
        && Objects.equals(longValue, that.longValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, metricId, longValue);
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(final String metricName) {
    this.metricName = metricName;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(final long metricId) {
    this.metricId = metricId;
  }

  public Long getLongValue() {
    return longValue;
  }

  public void setLongValue(final Long longValue) {
    this.longValue = longValue;
  }
}
