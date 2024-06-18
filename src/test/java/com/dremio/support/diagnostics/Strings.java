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
package com.dremio.support.diagnostics;

public class Strings {
  private static final int limit = 4096;

  public static int countSubstring(String mermaidText, String string) {
    int indexOf = 0;
    int count = 0;
    while (true) {
      if (count > limit) {
        throw new RuntimeException("too many found, throwing exception");
      }
      indexOf = mermaidText.indexOf(string, indexOf + 1);
      if (indexOf == -1) {
        break;
      }
      count++;
    }
    return count;
  }
}
