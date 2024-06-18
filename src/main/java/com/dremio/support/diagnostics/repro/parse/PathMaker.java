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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * PathMake is responsible for the path making logic in the reproduction tool this class sorts out
 * what goes into making spaces, folders and sources
 */
public class PathMaker {

  /**
   * escapes quotes, and ignores periods inside the quotes to generate a space and a folder path, do
   * not get the last one as it is the actual dataset example string "a.b".c.d becomes <code>
   * space: "a.b"
   * folders: new String[]{"a.b", "c"}
   * </code>
   *
   * @param datasetPath string to check
   * @return a space to create and then a list of folders underneath to make, note includes the top
   *     level space as well
   */
  public DatasetPath getListOfSpacesToMake(final String datasetPath) {
    List<List<String>> tokens = new ArrayList<>();
    StringBuilder currentToken = new StringBuilder();
    List<String> currentTokens = new ArrayList<>();
    int quotes = 0;
    for (int i = 0; i < datasetPath.length(); i++) {
      char c = datasetPath.charAt(i);
      if (c == '"') {
        quotes++;
        // we go ahead and remove them so we don't have them in path creation
        continue;
      }
      // if quotes is even it is safe to assume the '.' is not inside a double quote and
      // therefore escaped
      if (c == '.' && (quotes % 2 == 0)) {
        currentTokens.add(currentToken.toString());
        currentToken = new StringBuilder();
        tokens.add(new ArrayList<>(currentTokens));
        // don't store the dot
        continue;
      }
      // add all to the current token (except for the double quotes we skipped above)
      currentToken.append(c);
    }
    if (currentToken.length() > 0) {
      currentTokens.add(currentToken.toString());
      tokens.add(new ArrayList<>(currentTokens));
    }
    if (tokens.size() == 0) {
      throw new RuntimeException(
          String.format(
              "critical error there are no tokens for the path %s this is a bug", datasetPath));
    }
    String space = tokens.get(0).get(0);
    tokens.sort(Comparator.comparingInt(List::size));
    List<List<String>> foldersToBuild = new ArrayList<>();
    for (int i = 1; i < tokens.size() - 1; i++) {
      List<String> token = tokens.get(i);
      if (token.size() == 1) {
        continue;
      }
      foldersToBuild.add(token);
    }
    return new DatasetPath(space, foldersToBuild);
  }
}
