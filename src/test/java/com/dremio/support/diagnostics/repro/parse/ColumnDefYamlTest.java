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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

class ColumnDefYamlTest {

  @Test
  void testSetNull() {
    ColumnDefYaml yaml = new ColumnDefYaml();
    yaml.setTables(null);
    assertThat(yaml.getTables()).isNull();
  }

  @Test
  void testReadOnlyTableRef() {
    ColumnDefYaml yaml = new ColumnDefYaml();
    List<TableDef> tables = new ArrayList<>();
    yaml.setTables(tables);
    // now the two references should be separated
    tables.add(new TableDef());
    assertThat(yaml.getTables()).isEmpty();
  }

  @Nested
  class ParseYaml {
    private static ColumnDefYaml columnDefYaml;

    @BeforeAll
    static void setup() throws Exception {
      final Yaml yaml = new Yaml(new Constructor(ColumnDefYaml.class, new LoaderOptions()));
      try (final InputStream yamlFile =
          ParseYaml.class.getResourceAsStream("/com/dremio/support/diagnostics/column-def.yaml")) {
        columnDefYaml = yaml.load(yamlFile);
      }
    }

    @Test
    void testParseExampleYaml() {
      assertThat(columnDefYaml.getTables()).hasSize(2);
    }

    @Test
    void testHasColumnValues() {
      ColumnDef columnDef =
          columnDefYaml.getTables().stream()
              .filter(x -> "\"ns 1\".table1".equals(x.getName()))
              .findFirst()
              .get()
              .getColumns()
              .stream()
              .filter(x -> "accountNumber".equals(x.getName()))
              .findFirst()
              .get();
      List<String> list = new ArrayList<>();
      list.add("99");
      list.add("100");
      list.add("123451");
      list.add("123452");
      list.add("123453");
      list.add("123454");
      list.add("123455");
      list.add("123456");
      list.add("123457");
      assertThat(columnDef.getValues()).isEqualTo(list);
    }
  }
}
