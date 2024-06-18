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

import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import org.apache.commons.lang3.StringUtils;

public class BlockFinder {

  public PhaseBlockStats getUpstreamPhaseBlockStats(
      final String phase, final MinorFragmentProfile mostBlocked, final ProfileJSON profileJson) {
    long maxBlock = 0;
    long maxSleep = 0;
    long maxRunTime = 0;
    for (final FragmentProfile fragmentProfile : profileJson.getFragmentProfile()) {
      if (StringUtils.leftPad(String.valueOf(fragmentProfile.getMajorFragmentId()), 2, "0")
          .equals(phase)) {
        for (MinorFragmentProfile minorFragmentProfile :
            fragmentProfile.getMinorFragmentProfile()) {
          if (minorFragmentProfile.getBlockedOnUpstreamDuration() > maxBlock) {
            maxBlock = minorFragmentProfile.getBlockedOnUpstreamDuration();
          }
          if (minorFragmentProfile.getSleepingDuration() > maxSleep) {
            maxSleep = minorFragmentProfile.getSleepingDuration();
          }
          if (minorFragmentProfile.getRunDuration() > maxRunTime) {
            maxRunTime = minorFragmentProfile.getRunDuration();
          }
        }
      }
    }
    PhaseBlockStats phaseBlockStats = new PhaseBlockStats();
    phaseBlockStats.setPhase(phase);
    phaseBlockStats.setMaxBlockTime(maxBlock);
    phaseBlockStats.setMaxBlockTimePercentage(
        (maxBlock * 100.0f) / mostBlocked.getBlockedOnUpstreamDuration());
    phaseBlockStats.setRunTime(maxRunTime);
    phaseBlockStats.setRunTimePercentage(
        (maxRunTime * 100.0f) / mostBlocked.getBlockedOnUpstreamDuration());
    phaseBlockStats.setSleepTime(maxSleep);
    phaseBlockStats.setSleepTimePercentage(
        (maxSleep * 100.0f) / mostBlocked.getBlockedOnUpstreamDuration());
    return phaseBlockStats;
  }

  public PhaseBlockStats getDownstreamPhaseBlockStats(
      final String phase, final MinorFragmentProfile mostBlocked, final ProfileJSON profileJson) {
    long maxBlock = 0;
    long maxSleep = 0;
    long maxRunTime = 0;
    for (final FragmentProfile fragmentProfile : profileJson.getFragmentProfile()) {
      if (StringUtils.leftPad(String.valueOf(fragmentProfile.getMajorFragmentId()), 2, "0")
          .equals(phase)) {
        for (MinorFragmentProfile minorFragmentProfile :
            fragmentProfile.getMinorFragmentProfile()) {
          if (minorFragmentProfile.getBlockedOnDownstreamDuration() > maxBlock) {
            maxBlock = minorFragmentProfile.getBlockedOnDownstreamDuration();
          }
          if (minorFragmentProfile.getSleepingDuration() > maxSleep) {
            maxSleep = minorFragmentProfile.getSleepingDuration();
          }
          if (minorFragmentProfile.getRunDuration() > maxRunTime) {
            maxRunTime = minorFragmentProfile.getRunDuration();
          }
        }
      }
    }
    PhaseBlockStats phaseBlockStats = new PhaseBlockStats();
    phaseBlockStats.setPhase(phase);
    phaseBlockStats.setMaxBlockTime(maxBlock);
    phaseBlockStats.setMaxBlockTimePercentage(
        (maxBlock * 100.0f) / mostBlocked.getBlockedOnDownstreamDuration());
    phaseBlockStats.setRunTime(maxRunTime);
    phaseBlockStats.setRunTimePercentage(
        (maxRunTime * 100.0f) / mostBlocked.getBlockedOnDownstreamDuration());
    phaseBlockStats.setSleepTime(maxSleep);
    phaseBlockStats.setSleepTimePercentage(
        (maxSleep * 100.0f) / (float) mostBlocked.getBlockedOnDownstreamDuration());
    return phaseBlockStats;
  }
}
