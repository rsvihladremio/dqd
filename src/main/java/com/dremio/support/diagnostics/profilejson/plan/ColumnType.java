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

import java.util.List;
import java.util.Map;

public class ColumnType {
  private final String name;
  private final List<Integer> value;
  private final String complexType;

  private final Map<String, ColumnType> children;
  private final String typeName;

  /**
   * Type description of a column
   *
   * @param typeName name of the column type VARCHAR etc
   * @param name name of the column
   * @param value precision or value of the column: example VARCHAR(65336) 65336 is the value
   * @param complexType the optional complex type ARRAY etc for the column
   * @param children for a struct typically
   */
  public ColumnType(
      final String typeName,
      final String name,
      final List<Integer> value,
      final String complexType,
      final Map<String, ColumnType> children) {
    this.typeName = typeName;
    this.name = name;
    this.value = value;
    this.complexType = complexType;
    this.children = children;
  }

  /**
   * Name to return
   *
   * @return name of the column type VARCHAR etc
   */
  public String getName() {
    return name;
  }

  /**
   * value to of the column
   *
   * @return precision or value of the column: example VARCHAR(65336) 65336 is the value
   */
  public List<Integer> getValue() {
    return value;
  }

  /**
   * getter for the complex type of this column
   *
   * @return the optional complex type ARRAY etc for the column
   */
  public String getComplexType() {
    return complexType;
  }

  public Map<String, ColumnType> getChildren() {
    return children;
  }

  public String getTypeName() {
    return typeName;
  }
}
