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

import java.util.ArrayList;
import java.util.List;

public class ConvertToRelGraph {

  private final List<ConvertToRelNode> nodes;

  /**
   * gets all nodes in the for the convert to rel graph in ordered by top to bottom appearance
   *
   * @return list of nodes in a significant order. One can determine child parent relationship of
   *     the nodes by noting the indent or the out-dent of the subsequent node in the list. IE going
   *     from 4 in item one to 8 in item two would indicate that two is a child of one.
   */
  public List<ConvertToRelNode> getNodes() {
    return nodes;
  }

  /**
   * Represents the graph of generated by the Convert To Rel plan phase. This includes methods for
   * retrieving all children of a given node or finding problems inside of the graph to be resolved.
   *
   * @param nodes list of nodes in a signifcant order. One can determine child parent relationship
   *     of the nodes by noting the indent or the outdent of the subsequent node in the list. IE
   *     going from 4 in item one to 8 in item two would indicate that two is a child of one.
   */
  public ConvertToRelGraph(final List<ConvertToRelNode> nodes) {
    this.nodes = nodes;
  }

  public ConvertToRel getConvertToRelTree() {
    if (this.nodes.isEmpty()) {
      return null;
    }
    ConvertToRelNode parent = this.nodes.get(0);
    return mapFromNode(parent);
  }

  private boolean isChild(final int change) {
    return change > 0;
  }

  private boolean isDirectChild(final int change) {
    return change == 2;
  }

  private List<ConvertToRel> getChildrenForNode(final ConvertToRelNode node) {
    final List<ConvertToRel> children = new ArrayList<>();
    boolean countActive = false;
    for (ConvertToRelNode n : this.nodes) {
      if (countActive) {
        int change = n.getIndentDepth() - node.getIndentDepth();
        if (isChild(change)) {
          if (isDirectChild(change)) {
            children.add(mapFromNode(n));
          }
        } else {
          // stop counting when the child increases
          break;
        }
      }
      if (n.getId() == node.getId()) {
        countActive = true;
        continue;
      }
    }
    return children;
  }

  private ConvertToRel mapFromNode(final ConvertToRelNode node) {
    final List<ConvertToRel> children = getChildrenForNode(node);
    switch (node.getNodeType()) {
      case "ScanCrel":
        return new ScanCrel(children, node.getProperties());
      case "LogicalFilter":
        return new LogicalFilter(children, node.getProperties());
      case "LogicalJoin":
        return new LogicalJoin(children, node.getProperties());
      case "LogicalSort":
        return new LogicalSort(children, node.getProperties());
      default:
        return new ConvertToRel(node.getNodeType(), children, node.getProperties());
    }
  }
}
