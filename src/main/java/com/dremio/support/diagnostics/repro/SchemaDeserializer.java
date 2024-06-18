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

import com.dremio.support.diagnostics.shared.FileMaker;
import com.dremio.support.diagnostics.shared.dto.profilejson.DatasetProfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** interface for reading schema from the profile.json */
public interface SchemaDeserializer {

  /**
   * readSchema takes a schema in one format and returns it as clear text
   *
   * @param dp dataset profile to convert into a PDS
   */
  String readSchema(DatasetProfile dp);

  /** uses the tmp file system to create new directories */
  class TmpMaker implements FileMaker {
    /**
     * creates a directory from the tmp folder depending on the operating system and it's
     * configuration this may be automatically cleaned up or not
     *
     * @return the newly created directory
     * @throws IOException when the temp file system is not accessible or there is no space left on
     *     the device
     */
    @Override
    public Path getNewDir() throws IOException {
      return Files.createTempDirectory("dremio-repro-nfs");
    }
  }
}
