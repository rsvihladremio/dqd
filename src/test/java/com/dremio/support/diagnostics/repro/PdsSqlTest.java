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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PdsSqlTest {

  @Nested
  static class Getters {
    private static String tableName;
    private static String sql;
    private static PdsSql pdsSql;

    @BeforeAll
    static void initAll() {
      tableName = "testTable";
      sql = "SELECT * FROM bar";
      pdsSql = new PdsSql(tableName, sql);
    }

    @Test
    void testTableName() {
      assertEquals(pdsSql.getTableName(), tableName);
    }

    @Test
    void testSql() {
      assertEquals(pdsSql.getSql(), sql);
    }
  }

  @Nested
  static class TestEquals {

    private static String tableName;
    private static String sql;
    private static PdsSql pdsSql1;
    private static PdsSql pdsSql2;

    @BeforeAll
    static void initAll() {
      tableName = "testTable";
      sql = "SELECT * FROM bar";
      pdsSql1 = new PdsSql(tableName, sql);
      pdsSql2 = new PdsSql(tableName, sql);
    }

    @Test
    void testEqualsTheSameFieldsForTwoInstances() {
      assertEquals(pdsSql1, pdsSql2);
    }

    @Test
    void testEqualsMethod() {
      assertTrue(pdsSql1.equals(pdsSql2));
    }

    @Test
    void testEqualsSameInstanceMethod() {
      assertTrue(pdsSql1.equals(pdsSql1));
    }

    @Test
    void testEqualsNullObjectIsFalse() {
      PdsSql nullSql = null;
      assertFalse(pdsSql1.equals(nullSql));
    }

    @Test
    void testEmptyFields() {
      final PdsSql pdsSql3 = new PdsSql("", "");
      assertFalse(pdsSql1.equals(pdsSql3));
    }

    @Test
    void testSameTableButEmptySql() {
      final PdsSql pdsSql4 = new PdsSql(tableName, "");
      assertFalse(pdsSql1.equals(pdsSql4));
    }

    @Test
    void testSameSqlButEmptyTable() {
      final PdsSql pdsSql5 = new PdsSql("", sql);
      assertFalse(pdsSql1.equals(pdsSql5));
    }
  }

  @Test
  void testHashCode() {
    final PdsSql pds = new PdsSql("test", "select * from foo");
    assertEquals(pds.hashCode(), -292390503L);
  }

  @Test
  void testString() {
    final PdsSql pds = new PdsSql("test", "select * from foo");
    assertEquals(pds.toString(), "PdsSql{tableName='test', sql='select * from foo}");
  }
}
