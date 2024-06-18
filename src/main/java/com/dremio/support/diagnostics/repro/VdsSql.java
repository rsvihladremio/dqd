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

import java.util.Arrays;
import java.util.Objects;

/** VdsSql provides a vds, it's table, sql and the tables that it references */
public class VdsSql {
  private final String tableName;
  private final String sql;
  private final String[] tableReferences;

  /**
   * @param tableName table name of vds
   * @param sql sql used to create vds
   * @param tableReferences tables in the sql that are referenced by the vds
   */
  public VdsSql(final String tableName, final String sql, final String[] tableReferences) {
    this.tableName = tableName;
    this.sql = sql;
    this.tableReferences = tableReferences;
  }

  /**
   * getter for table name
   *
   * @return table name of the vds
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * getter for sql
   *
   * @return the sql used to create the vds
   */
  public String getSql() {
    return this.sql;
  }

  /**
   * getter for table references
   *
   * @return the table references
   */
  public String[] getTableReferences() {
    return this.tableReferences;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof VdsSql)) return false;
    VdsSql vdsSql = (VdsSql) o;
    return Objects.equals(tableName, vdsSql.tableName)
        && Objects.equals(sql, vdsSql.sql)
        && Arrays.equals(tableReferences, vdsSql.tableReferences);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(tableName, sql);
    result = 31 * result + Arrays.hashCode(tableReferences);
    return result;
  }

  /**
   * useful for debugging
   *
   * @return all the properties set in the VdsSql class
   */
  @Override
  public String toString() {
    return "VdsSql{"
        + "tableName='"
        + tableName
        + '\''
        + ", sql='"
        + sql
        + '\''
        + ", tableReferences="
        + Arrays.toString(tableReferences)
        + '}';
  }
}
