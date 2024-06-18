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
public class PerResourceBlockedDuration {

  private String resource;

  private int category;

  private long duration;

  public void setResource(final String resource) {
    this.resource = resource;
  }

  public String getResource() {
    return this.resource;
  }

  public void setCategory(final int category) {
    this.category = category;
  }

  public int getCategory() {
    return this.category;
  }

  public void setDuration(final long duration) {
    this.duration = duration;
  }

  public long getDuration() {
    return this.duration;
  }
}
