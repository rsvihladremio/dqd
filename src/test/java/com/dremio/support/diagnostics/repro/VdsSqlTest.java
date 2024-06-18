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

import static org.assertj.core.api.Assertions.assertThat;

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
      assertThat(vdsSql.getTableName()).isEqualTo(tableName);
    }

    @Test
    void testSql() {
      assertThat(vdsSql.getSql()).isEqualTo(sql);
    }

    @Test
    void testTableReferences() {
      assertThat(vdsSql.getTableReferences()).isEqualTo(tableReferences);
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
      assertThat(vdsSql1).isEqualTo(vdsSql2);
    }

    @Test
    void testEqualityTrue() {
      assertThat(vdsSql1.equals(vdsSql2)).isTrue();
    }

    @Test
    void testSameReferenceTrue() {
      assertThat(vdsSql1.equals(vdsSql1)).isTrue();
    }

    @Test
    void testNullCompareFalse() {
      VdsSql nullVds = null;
      assertThat(vdsSql1.equals(nullVds)).isFalse();
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    void testWrongTypeIsFalse() {
      assertThat(vdsSql1.equals(1L)).isFalse();
    }

    @Test
    void testEmptyStringsDontMatch() {
      VdsSql vdsSql3 = new VdsSql("", "", new String[] {});
      assertThat(vdsSql1.equals(vdsSql3)).isFalse();
    }

    @Test
    void testTableNameMatchesButNothingElse() {
      VdsSql vdsSql4 = new VdsSql(tableName, "", new String[] {});
      assertThat(vdsSql1.equals(vdsSql4)).isFalse();
    }

    @Test
    void testSqlMatchesButNothingElse() {
      VdsSql vdsSql5 = new VdsSql("", sql, new String[] {});
      assertThat(vdsSql1.equals(vdsSql5)).isFalse();
    }

    @Test
    void testTableFieldDoesntMatch() {
      VdsSql vdsSql6 = new VdsSql(tableName, sql, new String[] {"abc"});
      assertThat(vdsSql1.equals(vdsSql6)).isFalse();
    }
  }

  @Test
  void testHashCode() {
    VdsSql vds = new VdsSql("test", "select * from foo", new String[] {"abc"});
    assertThat(vds.hashCode()).isEqualTo(-474074616L);
  }

  @Test
  void testString() {
    VdsSql vds = new VdsSql("test", "select * from foo", new String[] {"test"});
    assertThat(vds.toString())
        .isEqualTo("VdsSql{tableName='test', sql='select * from foo', tableReferences=[test]}");
  }
}
