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

import com.google.common.base.Splitter;
import java.util.*;

/**
 * super naive implementation of a sql parser, this should be replaced as soon as possible but this
 * has been added for v1 to work. Known limitations include - no knowledge of comments and therefore
 * will be fooled - would not parse badly split files say for example "from abc" as a token will
 * match but the next token would be wrong - does not attempt to address all kinds of whitespace -
 * does not attempt to handle exhaustive sql
 */
public class TableRefFinder {

  /**
   * naive parser of table references, not a complete sql parser and can be fooled by comments
   *
   * @param statement sql statement to parse
   * @return finds all the from and join statements and gets the table referenced
   */
  public String[] searchSql(String statement) {
    final Set<String> references = new HashSet<>();
    final String cleaned = statement.toLowerCase(Locale.US).replace("\t", " ").replace("\n", " ");

    final Iterable<String> rawTokens = Splitter.on(' ').split(cleaned);
    List<String> tokenList = new ArrayList<>();
    for (String token : rawTokens) {

      // remove all empty tokens
      if (!"".equals(token)) {
        tokenList.add(token);
      }
    }
    String[] tokens = tokenList.toArray(new String[0]);
    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      // remove all current_date function references

      if (token.trim().equalsIgnoreCase("from") || token.trim().equalsIgnoreCase("join")) {

        // make sure to skip if there is no next token
        if (i + 1 == tokens.length) {
          break;
        }
        // now find the next token
        String ref = tokens[i + 1].trim().replace("\n", "").replace(")", "");
        if (ref.contains("current_date")
            && !ref.equals("\"current_date\"")
            && (ref.contains(")")
                || ref.contains("<")
                || ref.contains("=")
                || ref.contains(">")
                || ref.contains("-")
                || ref.contains(","))) {
          continue;
        }
        // strip out quotes for proper comparisions downstream
        String cleanedRef = ref.replace("\"", "");
        if (cleanedRef.startsWith("(")) {
          // skip all nested queries
          continue;
        }

        references.add(cleanedRef);
      }
    }
    return references.toArray(new String[0]);
  }
}
