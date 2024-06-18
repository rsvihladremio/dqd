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
package com.dremio.support.diagnostics.profilejson.plan;

import static org.assertj.core.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

public class ColumnTypeTest {

  @Test
  void testGetters() {
    String childColName = "myChildCol";
    String name = "myChild";
    String typeName = "myName";
    String arrayType = "ARRAY";
    List<Integer> values = new ArrayList<>();
    values.add(1);
    values.add(2);
    Map<String, ColumnType> children = new HashMap<>();
    ColumnType child = new ColumnType("mychild", childColName, values, "", new HashMap<>());

    children.put(childColName, child);
    ColumnType t = new ColumnType(typeName, name, values, arrayType, children);
    assertThat(t.getTypeName()).isEqualTo(typeName);
    assertThat(t.getName()).isEqualTo(name);
    assertThat(t.getValue()).isEqualTo(Arrays.asList(1, 2));
    assertThat(t.getComplexType()).isEqualTo(arrayType);
    assertThat(t.getChildren().size()).isEqualTo(1);
    assertThat(t.getChildren().get(childColName).getChildren()).isEmpty();
    assertThat(t.getChildren().get(childColName).getValue()).isEqualTo(values);
    assertThat(t.getChildren().get(childColName).getComplexType()).isEqualTo("");
    assertThat(t.getChildren().get(childColName).getName()).isEqualTo(child.getName());
    assertThat(t.getChildren().get(childColName).getTypeName()).isEqualTo(child.getTypeName());
  }
}
