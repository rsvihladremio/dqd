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

/**
 * core difference class provides the type of difference (name), the values of each profile, and a
 * suggested action of analysis of the difference (called advice)
 */
public class Difference {
  private String name;
  private String advice;
  private String profile1Value;
  private String profile2Value;

  /**
   * getter for name
   *
   * @return name or title of the difference
   */
  public String getName() {
    return name;
  }

  /**
   * sets the name
   *
   * @param name name or title of the difference
   */
  public void setName(final String name) {
    this.name = name;
  }

  /**
   * gets advice
   *
   * @return advice for next steps or information around the difference
   */
  public String getAdvice() {
    return advice;
  }

  /**
   * sets advice for the data displayed in the column next to this
   *
   * @param advice is an optional field to advise on next steps
   */
  public void setAdvice(final String advice) {
    this.advice = advice;
  }

  public String getProfile1Value() {
    return profile1Value;
  }

  public void setProfile1Value(final String profile1Value) {
    this.profile1Value = profile1Value;
  }

  public String getProfile2Value() {
    return profile2Value;
  }

  public void setProfile2Value(final String profile2Value) {
    this.profile2Value = profile2Value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Difference)) return false;
    Difference that = (Difference) o;
    return Objects.equals(name, that.name)
        && Objects.equals(advice, that.advice)
        && Objects.equals(profile1Value, that.profile1Value)
        && Objects.equals(profile2Value, that.profile2Value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, advice, profile1Value, profile2Value);
  }

  @Override
  public String toString() {
    return "Difference{"
        + "name='"
        + name
        + '\''
        + ", advice='"
        + advice
        + '\''
        + ", Profile1Value='"
        + profile1Value
        + '\''
        + ", Profile2Value='"
        + profile2Value
        + '\''
        + '}';
  }
}
