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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ColumnDefTest {

  @Test
  void testSetNull() {
    ColumnDef columnDef = new ColumnDef();
    columnDef.setValues(null);
    assertThat(columnDef.getValues()).isNull();
  }

  @Test
  void testReadOnlyColumnValues() {
    ColumnDef columnDef = new ColumnDef();
    List<String> values = new ArrayList<>();
    columnDef.setValues(values);
    // now the two references should be separated
    values.add("value1");
    assertThat(columnDef.getValues()).isEmpty();
  }

  @Test
  void testGeSettName() {
    ColumnDef columnDef = new ColumnDef();
    columnDef.setName("myName");
    assertThat(columnDef.getName()).isEqualTo("myName");
  }

  @Nested
  static class GetSetValues {
    private static ColumnDef columnDef;

    @BeforeAll
    static void setup() {
      columnDef = new ColumnDef();
      List<String> values = new ArrayList<>();
      values.add("A");
      values.add("B");
      columnDef.setValues(values);
    }

    @Test
    void testHas2Items() {
      assertThat(columnDef.getValues()).hasSize(2);
    }

    @Test
    void testHasFirstElement() {
      assertThat(columnDef.getValues().get(0)).isEqualTo("A");
    }

    @Test
    void testHasSecondElement() {
      assertThat(columnDef.getValues().get(1)).isEqualTo("B");
    }
  }
}
