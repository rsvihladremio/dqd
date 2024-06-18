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
package com.dremio.support.diagnostics.shared;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.stream.Stream;

public class PathParser {

  /**
   * reads a path, if it is a directory..lists all files in the directory if is a list of comma
   * separated files (with or without spaces) it will convert them to a string array if it is just a
   * single file it will read it as is without any trimming
   *
   * @param path the path that can be a directory, a list of files separated by commas or a single
   *     file
   * @return a list of files
   */
  public String[] convertPathToFiles(String path) throws FileNotFoundException {
    File filePath = new File(path);
    if (filePath.isDirectory()) {
      // note by entering here it is not necessary to check if the file exists
      // as it must exist to return true to be a directory
      // read all files inside dir
      return Stream.of(Objects.requireNonNull(filePath.listFiles())) // list all files
          .map(File::getPath) // only consider json files
          .filter(xPath -> xPath.endsWith(".json")) // convert to path
          .toArray(String[]::new);
    } else if (path.contains(",")) {
      // split up into files
      String[] paths = path.split(",");
      // clean and trim paths
      for (int i = 0; i < paths.length; i++) {
        paths[i] = paths[i].trim();
        // validate the files are valid
        validateExists(new File(paths[i]));
      }
      return paths;
    } else {
      // validate it exists first
      validateExists(filePath);
      // just return as a single file
      return new String[] {path};
    }
  }

  private void validateExists(File filePath) throws FileNotFoundException {
    if (!filePath.exists()) {
      throw new FileNotFoundException("unable to file or directory:  " + filePath);
    }
  }
}
