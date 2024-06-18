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
package com.dremio.support.diagnostics.repro.parse;

import java.util.List;

/**
 * represents a dataset path object, one is the space and one are the folders under the space the
 * folders
 */
public class DatasetPath {
  private final String space;
  private final List<List<String>> folders;

  /**
   * dataset path is a period separated text string that explains the path to a given dataset
   *
   * @param space space to create
   * @param folders list of folders in order to create, will be empty of quotes
   */
  public DatasetPath(String space, List<List<String>> folders) {
    this.space = space;
    this.folders = folders;
  }

  /**
   * list of folders derived from the dataset to make
   *
   * @return list of folders in order to create
   */
  public List<List<String>> getFolders() {
    return folders;
  }

  /**
   * the space to make before making the folders
   *
   * @return top level space object
   */
  public String getSpace() {
    return space;
  }
}
