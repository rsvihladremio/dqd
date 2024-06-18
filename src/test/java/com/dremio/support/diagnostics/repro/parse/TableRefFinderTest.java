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

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TableRefFinderTest {

  private final TableRefFinder finder = new TableRefFinder();
  private final String fooTest = "foo.test";
  private final String abcEfg = "abc.efg";

  @Test
  void testBasicSQLWithFrom() {
    String[] tables = finder.searchSql("SELECT * FROM abc.efg");
    assertThat(tables).isEqualTo(new String[] {abcEfg});
  }

  @Test
  void testFormattedSQLWithFrom() {
    String[] tables = finder.searchSql("SELECT *\nFROM abc.efg");
    assertThat(tables).isEqualTo(new String[] {abcEfg});
  }

  @Test
  void testBadlyFormattedSQLWithFrom() {
    String[] tables = finder.searchSql("SELECT *\n  FROM abc.efg");
    assertThat(tables).isEqualTo(new String[] {abcEfg});
  }

  @Test
  void testBadlyFormattedSQLJJoinAndFrom() {
    String[] tables =
        finder.searchSql(
            "SELECT *\n  FROM abc.efg\n  INNER JOIN jointable WHERE abc.efg.id = jointable.id");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {abcEfg, "jointable"});
  }

  @Test
  void testSkipNestedFrom() {
    String[] tables = finder.searchSql("SELECT *\n  FROM \n(SELECT * FROM foo.test )");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {fooTest});
  }

  @Test
  void testSkipNestedFromStripParens() {
    String[] tables = finder.searchSql("SELECT *\n  FROM \n(SELECT * FROM foo.test)");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {fooTest});
  }

  @Test
  void testRemoveDoubleQuotes() {
    String[] tables = finder.searchSql("SELECT *\n  FROM \n(SELECT * FROM \"foo.test\")");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {fooTest});
  }

  @Test
  void testRemoveCasing() {
    String[] tables = finder.searchSql("SELECT *\n  FROM \n(SELECT * FROM \"FOO.test\")");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {fooTest});
  }

  @Test
  void testStripCurrentDateFunction() {
    String[] tables =
        finder.searchSql(
            "SELECT * (\n"
                + "  FROM \n"
                + "(SELECT month from current_date,test,* FROM \"foo.test\" WHEN extract(month from"
                + " current_date)>1 OR extract(month from current_date)<1 OR extra(month from"
                + " current_date)=1)");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {fooTest});
  }

  @Test
  void testDontStripCurrentDateTables() {
    String[] tables = finder.searchSql("SELECT * (\n" + "  FROM \n" + "\"current_date\"");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {"current_date"});
  }

  @Test
  void testDontStripCurrentTablesWithCurrentDateInTheName() {
    String[] tables = finder.searchSql("SELECT * FROM total_current_date");
    Arrays.sort(tables);
    assertThat(tables).isEqualTo(new String[] {"total_current_date"});
  }
}
