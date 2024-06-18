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
package com.dremio.support.diagnostics.profilejson.singlefile;

public class PhaseBlockStats {
  private String phase;
  private long maxBlockTime;
  private float maxBlockTimePercentage;
  private long runTime;
  private float runTimePercentage;
  private long sleepTime;
  private float sleepTimePercentage;

  public String getPhase() {
    return phase;
  }

  public void setPhase(String phase) {
    this.phase = phase;
  }

  public long getMaxBlockTime() {
    return maxBlockTime;
  }

  public void setMaxBlockTime(long maxBlockTime) {
    this.maxBlockTime = maxBlockTime;
  }

  public float getMaxBlockTimePercentage() {
    return maxBlockTimePercentage;
  }

  public void setMaxBlockTimePercentage(float maxBlockTimePercentage) {
    this.maxBlockTimePercentage = maxBlockTimePercentage;
  }

  public long getRunTime() {
    return runTime;
  }

  public void setRunTime(long runTime) {
    this.runTime = runTime;
  }

  public float getRunTimePercentage() {
    return runTimePercentage;
  }

  public void setRunTimePercentage(float runTimePercentage) {
    this.runTimePercentage = runTimePercentage;
  }

  public long getSleepTime() {
    return sleepTime;
  }

  public void setSleepTime(long sleepTime) {
    this.sleepTime = sleepTime;
  }

  public float getSleepTimePercentage() {
    return sleepTimePercentage;
  }

  public void setSleepTimePercentage(float sleepTimePercentage) {
    this.sleepTimePercentage = sleepTimePercentage;
  }
}
