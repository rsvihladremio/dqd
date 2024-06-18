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

public class TableDef {

  private List<ColumnDef> columns;
  private String name;

  /**
   * getter for the column column definitions
   *
   * @return a read only list of column definitions
   */
  public List<ColumnDef> getColumns() {
    return columns;
  }

  /**
   * setter for list of column definitions
   *
   * @param columns the list of column definitions
   */
  public void setColumns(List<ColumnDef> columns) {
    if (columns != null) {
      this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    }
  }

  /**
   * gettier for the name of the table
   *
   * @return name of the table
   */
  public String getName() {
    return name;
  }

  /**
   * setter for the name of the table
   *
   * @param name name of the column
   */
  public void setName(String name) {
    this.name = name;
  }
}
