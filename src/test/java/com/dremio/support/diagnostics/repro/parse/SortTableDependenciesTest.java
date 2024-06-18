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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.support.diagnostics.repro.VdsSql;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SortTableDependenciesTest {

  private static final String table1 = "table1";
  private static final String table2 = "table2";

  @Nested
  static class SortTableDependenciesHappyPath {
    private static List<VdsSql> vdss;
    private static VdsSql vdsSql2;
    private static VdsSql vdsSql1;

    @BeforeAll
    static void beforeAll() {
      vdsSql1 = new VdsSql(table1, "", new String[] {table2});
      vdsSql2 = new VdsSql(table2, "", new String[] {});
      vdss = Arrays.asList(vdsSql1, vdsSql2);
      SortTableDependencies sort = new SortTableDependencies();
      // expected order would be table2, table1
      sort.sortVds(vdss);
    }

    @Test
    void testVdsWithNoDependenciesIsFirst() {
      assertThat(vdss.get(0)).isEqualTo(vdsSql2);
    }

    @Test
    void testVdsWithDependenciesOnOtherTableGoesAfter() {
      assertThat(vdss.get(1)).isEqualTo(vdsSql1);
    }
  }

  @Nested
  static class SortingWhenTablesAreAlreadyInCorrectOrder {
    private static List<VdsSql> vdss;
    private static VdsSql vdsSql2;
    private static VdsSql vdsSql1;

    @BeforeAll
    static void beforeAll() {
      vdsSql1 = new VdsSql(table1, "", new String[] {});
      vdsSql2 = new VdsSql(table2, "", new String[] {table1});
      vdss = Arrays.asList(vdsSql1, vdsSql2);
      SortTableDependencies sort = new SortTableDependencies();
      // expected order would be table1, table2
      sort.sortVds(vdss);
    }

    @Test
    void testFirstTableIsStillFirst() {
      assertThat(vdss.get(0)).isEqualTo(vdsSql1);
    }

    @Test
    void testFirstTableIsStillSecond() {
      assertThat(vdss.get(1)).isEqualTo(vdsSql2);
    }
  }

  @Nested
  static class SortTableDependenciesInOrderComplexCase {

    private static List<VdsSql> vdss;
    private static VdsSql vdsSql3;
    private static VdsSql vdsSql2;
    private static VdsSql vdsSql1;

    @BeforeAll
    static void beforeAll() {

      vdsSql1 = new VdsSql(table1, "", new String[] {table2});
      vdsSql2 =
          new VdsSql(
              table2,
              "",
              new String[] {
                "table3",
              });
      vdsSql3 = new VdsSql("table3", "", new String[] {});
      vdss = Arrays.asList(vdsSql1, vdsSql2, vdsSql3);
      SortTableDependencies sort = new SortTableDependencies();
      sort.sortVds(vdss);
      // expected order would be table2, table1, table3
    }

    @Test
    void testTableWithNoDependenciesFirst() {
      assertThat(vdss.get(0)).isEqualTo(vdsSql3);
    }

    @Test
    void testTableThatUsesFirstTableIsSecond() {
      assertThat(vdss.get(1)).isEqualTo(vdsSql2);
    }

    @Test
    void testTableThatDependsOnSecondTableOnlyIsThird() {
      assertThat(vdss.get(2)).isEqualTo(vdsSql1);
    }
  }
}
