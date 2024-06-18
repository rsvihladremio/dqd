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
package com.dremio.support.diagnostics.profilejson.singlefile.reports.summary;

public class OperatorRecordDetail {
  private String name;
  private long runTimeNanos;
  private long records;
  private long batches;
  private String condition;
  private long peakLocalMemoryAllocated;

  public String getCondition() {
    return condition;
  }

  public void setCondition(final String condition) {
    this.condition = condition;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public long getRunTimeNanos() {
    return runTimeNanos;
  }

  public void setRunTimeNanos(final long runTimeNanos) {
    this.runTimeNanos = runTimeNanos;
  }

  public long getRecords() {
    return records;
  }

  public void setRecords(final long records) {
    this.records = records;
  }

  public long getBatches() {
    return batches;
  }

  public void setBatches(final long batches) {
    this.batches = batches;
  }

  public double getRecordsPerSecond() {
    return this.getRecords() / (this.getRunTimeNanos() / 1000000000.0);
  }

  public void setPeakLocalMemoryAllocated(final long peakLocalMemoryAllocated) {
    this.peakLocalMemoryAllocated = peakLocalMemoryAllocated;
  }

  public long getPeakLocalMemoryAllocated() {
    return peakLocalMemoryAllocated;
  }
}
