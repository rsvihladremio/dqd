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

import com.dremio.support.diagnostics.repro.VdsSorter;
import com.dremio.support.diagnostics.repro.VdsSql;
import java.util.List;
import java.util.Locale;

/**
 * sorts the vds by table references so that the VDSs can be executed in order that will allow them
 * to succeed (assuming there is no orphaned references
 */
public class SortTableDependencies implements VdsSorter {

  private static final Locale locale = Locale.US;

  /**
   * sort the VDS list passed in. This ATTEMPTS to search the list of table references in the query,
   * however as there are a large variable number of queries this often fails. This was chosen
   * instead of a full sql parser that would require matching versions of dremio to the sql text.
   * This was judged too difficult and time consuming at the time. Only trust this as a best effort.
   *
   * @param vds list of VDSs to sort this is mutated by reference
   */
  @Override
  public void sortVds(List<VdsSql> vds) {
    vds.sort(
        (v1, v2) -> {
          for (String ref : v2.getTableReferences()) {
            if (ref.toLowerCase(locale)
                .replace("\"", "")
                .equals(v1.getTableName().toLowerCase(locale).replace("\"", ""))) {
              return -1;
            }
          }
          for (String ref : v1.getTableReferences()) {
            if (ref.toLowerCase(locale)
                .replace("\"", "")
                .equals(v2.getTableName().toLowerCase(locale).replace("\"", ""))) {
              return 1;
            }
          }
          return 0;
        });
  }
}
