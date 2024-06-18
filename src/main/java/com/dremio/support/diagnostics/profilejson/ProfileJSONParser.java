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

import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.PerResourceBlockedDuration;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;

/** ProfileJSONParser reads a json file and converts it to a json object */
public class ProfileJSONParser implements Parser {

  /**
   * this delegates to the jackson file parser so it should be as memory efficient as possible
   *
   * @param file file to parse
   * @return a parsed ProfileJSON
   */
  @Override
  public ProfileJSON parseFile(final InputStream file) throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    ProfileJSON profileJson = objectMapper.readValue(file, ProfileJSON.class);
    if (profileJson.getFragmentProfile() != null) {
      // correct time in minor profile before we can rely on it.. this is stealing
      // code from
      // https://github.com/dremio/dremio-oss/blame/master/dac/backend/src/main/java/com/dremio/dac/server/admin/profile/FragmentWrapper.java
      for (final FragmentProfile fragmentProfile : profileJson.getFragmentProfile()) {
        if (fragmentProfile == null) {
          continue;
        }
        for (final MinorFragmentProfile minor : fragmentProfile.getMinorFragmentProfile()) {
          if (minor == null) {
            continue;
          }
          correctDuration(minor);
        }
      }
    }
    return profileJson;
  }

  private static final int UPSTREAM = 0;
  private static final int DOWNSTREAM = 1;

  private void correctDuration(MinorFragmentProfile minor) {
    if (minor == null) {
      return;
    }
    long blockedOnDownstreamDuration = minor.getBlockedOnDownstreamDuration();
    long blockedOnUpstreamDuration = minor.getBlockedOnUpstreamDuration();
    long blockedOnOtherDuration = 0;
    if (minor.getPerResourceBlockedDuration() != null) {
      for (PerResourceBlockedDuration resourceDuration : minor.getPerResourceBlockedDuration()) {
        switch (resourceDuration.getCategory()) {
          case UPSTREAM:
            blockedOnUpstreamDuration += resourceDuration.getDuration();
            break;
          case DOWNSTREAM:
            blockedOnDownstreamDuration += resourceDuration.getDuration();
            break;
          default:
            blockedOnOtherDuration += resourceDuration.getDuration();
            break;
        }
      }
    }
    minor.setBlockedOnSharedResourceDuration(blockedOnOtherDuration);
    minor.setBlockedOnUpstreamDuration(blockedOnUpstreamDuration);
    minor.setBlockedOnDownstreamDuration(blockedOnDownstreamDuration);
  }
}
