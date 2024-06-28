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

@SuppressWarnings({"ConstantConditions", "EqualsBetweenInconvertibleTypes"})
class VdsSqlTest {

  @Nested
  static class Getters {
    private static String tableName;
    private static String sql;
    private static String[] tableReferences;
    private static VdsSql vdsSql;

    @BeforeAll
    static void initAll() {
      tableName = "testTable";
      sql = "SELECT * FROM bar";
      tableReferences = new String[] {"table1", "table2", "table3"};
      vdsSql = new VdsSql(tableName, sql, tableReferences);
    }

    @Test
    void testTableName() {
      assertEquals(vdsSql.getTableName(), tableName);
    }

    @Test
    void testSql() {
      assertEquals(vdsSql.getSql(), sql);
    }

    @Test
    void testTableReferences() {
      assertEquals(vdsSql.getTableReferences(), tableReferences);
    }
  }

  @Nested
  static class ValidateEquals {
    private static VdsSql vdsSql1;
    private static VdsSql vdsSql2;
    private static String[] tableReferences;
    private static String sql;
    private static String tableName;

    @BeforeAll
    static void initAll() {
      tableName = "testTable";
      sql = "SELECT * FROM bar";
      tableReferences = new String[] {"table1", "table2", "table3"};
      vdsSql1 = new VdsSql(tableName, sql, tableReferences);
      vdsSql2 = new VdsSql(tableName, sql, tableReferences);
    }

    @Test
    void testEquals() {
      assertEquals(vdsSql1, vdsSql2);
    }

    @Test
    void testEqualityTrue() {
      assertTrue(vdsSql1.equals(vdsSql2));
    }

    @Test
    void testSameReferenceTrue() {
      assertTrue(vdsSql1.equals(vdsSql1));
    }

    @Test
    void testNullCompareFalse() {
      VdsSql nullVds = null;
      assertFalse(vdsSql1.equals(nullVds));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    void testWrongTypeIsFalse() {
      assertFalse(vdsSql1.equals(1L));
    }

    @Test
    void testEmptyStringsDontMatch() {
      VdsSql vdsSql3 = new VdsSql("", "", new String[] {});
      assertFalse(vdsSql1.equals(vdsSql3));
    }

    @Test
    void testTableNameMatchesButNothingElse() {
      VdsSql vdsSql4 = new VdsSql(tableName, "", new String[] {});
      assertFalse(vdsSql1.equals(vdsSql4));
    }

    @Test
    void testSqlMatchesButNothingElse() {
      VdsSql vdsSql5 = new VdsSql("", sql, new String[] {});
      assertFalse(vdsSql1.equals(vdsSql5));
    }

    @Test
    void testTableFieldDoesntMatch() {
      VdsSql vdsSql6 = new VdsSql(tableName, sql, new String[] {"abc"});
      assertFalse(vdsSql1.equals(vdsSql6));
    }
  }

  @Test
  void testHashCode() {
    VdsSql vds = new VdsSql("test", "select * from foo", new String[] {"abc"});
    assertEquals(vds.hashCode(), -474074616L);
  }

  @Test
  void testString() {
    VdsSql vds = new VdsSql("test", "select * from foo", new String[] {"test"});
    assertEquals(
        vds.toString(),
        "VdsSql{tableName='test', sql='select * from foo', tableReferences=[test]}");
  }
}
