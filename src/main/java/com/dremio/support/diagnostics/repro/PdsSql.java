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

import java.util.Objects;

/** PdsSql provides a pds it's table and sql */
public class PdsSql {

  private final String tableName;
  private final String sql;

  /**
   * @param tableName table name of vds
   * @param sql sql used to create vds
   */
  public PdsSql(final String tableName, final String sql) {
    this.tableName = tableName;
    this.sql = sql;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PdsSql)) return false;
    PdsSql pdsSql = (PdsSql) o;
    return Objects.equals(tableName, pdsSql.tableName) && Objects.equals(sql, pdsSql.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, sql);
  }

  /**
   * useful for debugging
   *
   * @return all the properties set in the VdsSql class
   */
  @Override
  public String toString() {
    return "PdsSql{" + "tableName='" + tableName + '\'' + ", sql='" + sql + '}';
  }
}
