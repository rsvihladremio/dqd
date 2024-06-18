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
import java.util.Collections;
import java.util.List;

public class ColumnDefYaml {

  private List<TableDef> tables;

  /**
   * getter for table defs
   *
   * @return a read only list of table defnitions
   */
  public List<TableDef> getTables() {
    return tables;
  }

  /**
   * setter for list of table def
   *
   * @param tables the table definitions to set
   */
  public void setTables(final List<TableDef> tables) {
    if (tables != null) {
      this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
    }
  }
}
