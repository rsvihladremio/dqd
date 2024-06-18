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

import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.util.Collection;

/** interface for parsing a profile json into various objects */
public interface ReproProfileParser {

  /**
   * returns list of vds
   *
   * @param profileJSON parsed json
   * @return vds objects needed to output something useful
   */
  Collection<VdsSql> parseVDSs(ProfileJSON profileJSON);

  /**
   * returns list of PDS as sql statements (for example as CTAS)
   *
   * @param profileJSON parsed profile json
   * @return generated SQL for PDSs
   */
  Collection<PdsSql> parsePDSs(ProfileJSON profileJSON);

  /**
   * returns spaces to create in required order of creation
   *
   * @param profileJSON parsed profile json
   * @return spaces to create
   */
  Collection<String> parseSpaces(ProfileJSON profileJSON);

  /**
   * returns folders to create in required order of creation
   *
   * @param profileJSON parsed profile json
   * @return folders to create
   */
  Collection<Collection<String>> parseFolders(ProfileJSON profileJSON);

  /**
   * returns sources to create
   *
   * @param profileJSON parsed profile json
   * @return sources to create
   */
  Collection<String> parseSources(ProfileJSON profileJSON);
}
