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
package com.dremio.support.diagnostics.repro;

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Objects;

/**
 * Job result of an operation. For example trying to submit spaces to the api show which ones failed
 * and succeeded. If the job failed critically, list the reason why
 */
public class JobResult {
  private boolean success;
  private Collection<String> added;
  private String failure;

  @Override
  public String toString() {
    return "JobResult{"
        + "success="
        + success
        + ", added="
        + added
        + ", failure='"
        + failure
        + '\''
        + '}';
  }

  /**
   * getter which tells if you if job was successful
   *
   * @return if the process was successful or not
   */
  public boolean getSuccess() {
    return success;
  }

  /**
   * setter for added list. This typically will represent the work that was processed. Typically
   * when writing to a server.
   *
   * @param added spaces that were successfully added
   */
  public void added(final Collection<String> added) {

    this.added = added;
  }

  /**
   * setter for success
   *
   * @param success spaces that were successfully added
   */
  public void setSuccess(final boolean success) {
    this.success = success;
  }

  /**
   * setter for error message
   *
   * @param failure stores an error message
   */
  public void setFailure(final String failure) {
    this.failure = failure;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof JobResult)) return false;
    JobResult jobResult = (JobResult) o;
    return success == jobResult.success
        && Iterables.elementsEqual(added, jobResult.added)
        && Objects.equals(failure, jobResult.failure);
  }

  @Override
  public int hashCode() {
    return Objects.hash(success, added, failure);
  }
}
