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
package com.dremio.support.diagnostics.profilejson.converttorel;

import java.util.Map;

public class ConvertToRelNode {

  private final String nodeType;
  private final Map<String, String> properties;
  private final int indentDepth;
  private final int id;

  /**
   * Represents a line inside of the Convert To Rel plan phase. Each "line" in the plan phase
   * represents a node in the plan phase's graph
   *
   * @param id id for creating the graph, while parsing this is just an incremented value
   * @param nodeType name of the Convert To Rel node ie LogicalProject, LogicalFilter, ScanCrel
   * @param properties properties containg interested data points for the node
   * @param indentDepth used to determine relationships since the graph is read in order a
   *     increasing indentation suggests "child" from the previous node ie the "parent"
   */
  public ConvertToRelNode(
      final int id,
      final String nodeType,
      final Map<String, String> properties,
      final int indentDepth) {
    this.id = id;
    this.nodeType = nodeType;
    this.properties = properties;
    this.indentDepth = indentDepth;
  }

  /**
   * getter for id
   *
   * @return id for the node, generated during parsing
   */
  public int getId() {
    return id;
  }

  /**
   * getter for the node type
   *
   * @return name of the Convert To Rel node ie LogicalProject, LogicalFilter, ScanCrel
   */
  public String getNodeType() {
    return nodeType;
  }

  /**
   * getter for the node properties
   *
   * @return properties containing interested data points for the node
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * the number of spaces used for indention in the convert to rel text for this node
   *
   * @return used to determine relationships since the graph is read in order a increasing
   *     indentation suggests "child" from the previous node ie the "parent"
   */
  public int getIndentDepth() {
    return indentDepth;
  }

  @Override
  public String toString() {
    return "ConvertToRelNode [id="
        + id
        + ", nodeType="
        + nodeType
        + ", properties="
        + properties
        + ", indentDepth="
        + indentDepth
        + "]";
  }
}
