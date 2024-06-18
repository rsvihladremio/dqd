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
public class DatasetProfile {

  private String sql;

  private String datasetPath;

  private long type;

  private String batchSchema;

  private boolean allowApproxStats;

  public String getSql() {
    return sql;
  }

  public void setSql(final String sql) {
    this.sql = sql;
  }

  public void setDatasetPath(final String datasetPath) {
    this.datasetPath = datasetPath;
  }

  public String getDatasetPath() {
    return this.datasetPath;
  }

  public void setType(final long type) {
    this.type = type;
  }

  public long getType() {
    return this.type;
  }

  public void setBatchSchema(final String batchSchema) {
    this.batchSchema = batchSchema;
  }

  public String getBatchSchema() {
    return this.batchSchema;
  }

  public void setAllowApproxStats(final boolean allowApproxStats) {
    this.allowApproxStats = allowApproxStats;
  }

  public boolean getAllowApproxStats() {
    return this.allowApproxStats;
  }
}
