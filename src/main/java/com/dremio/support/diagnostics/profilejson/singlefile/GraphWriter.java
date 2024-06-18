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
package com.dremio.support.diagnostics.profilejson.singlefile;

import com.dremio.support.diagnostics.profilejson.converttorel.ConvertToRel;
import com.dremio.support.diagnostics.profilejson.converttorel.LogicalFilter;
import com.dremio.support.diagnostics.profilejson.converttorel.LogicalJoin;
import com.dremio.support.diagnostics.profilejson.converttorel.LogicalSort;
import com.dremio.support.diagnostics.profilejson.converttorel.ScanCrel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphWriter {

  public String writeMermaid(final ConvertToRel convertToRel) {
    id = 0;
    LabelData labelData = writeNodeLabel(convertToRel);
    StringBuilder builder = new StringBuilder();
    builder.append("<pre class=\"mermaid\">\ngraph TD;\n");
    String[] links =
        Arrays.stream(labelData.getLinks()).map(x -> String.format("%s", x)).toArray(String[]::new);
    builder.append(String.join("\n", links));
    builder.append("\n");
    String[] data = Arrays.stream(labelData.getAllCustomData()).toArray(String[]::new);
    builder.append(String.join("\n", data));
    builder.append("\n");
    builder.append("</pre>\n");
    return builder.toString();
  }

  private int id = 0;

  private LabelData writeNodeLabel(ConvertToRel node) {
    List<LabelData> children = new ArrayList<>();

    String typeName = node.getTypeName();
    LabelData label;
    switch (typeName) {
      case "ScanCrel":
        {
          ScanCrel scanCrel = (ScanCrel) node;
          long splits = scanCrel.getSplits();
          String[] columns = scanCrel.getColumns();
          Map<String, String> customData = new HashMap<>();
          customData.put("columns", String.join(", ", columns));
          label = new LabelData(id, id + "-ScanCrel\\nsplits:" + splits, children, customData);
          break;
        }
      case "LogicalSort":
        {
          LogicalSort logicalSort = (LogicalSort) node;
          long fetch = logicalSort.getFetch();
          label =
              new LabelData(id, id + "-LogicalSort\\nfetch:" + fetch, children, new HashMap<>());
          break;
        }
      case "LogicalJoin":
        {
          LogicalJoin logicalJoin = (LogicalJoin) node;
          String condition = logicalJoin.getCondition();
          String joinType = logicalJoin.getJoinType();
          Map<String, String> customData = new HashMap<>();
          customData.put("condition", condition);
          label = new LabelData(id, id + "-LogicalJoin\\ntype:" + joinType, children, customData);
          break;
        }
      case "LogicalFilter":
        {
          LogicalFilter logicalFilter = (LogicalFilter) node;
          // TODO: maybe split up all conditions into separate lines?
          String condition = logicalFilter.getCondition();
          Map<String, String> customData = new HashMap<>();
          customData.put("condition", condition);
          label = new LabelData(id, id + "-LogicalFilter", children, customData);
          break;
        }
      default:
        {
          // v1 we are tossing this, but would like to make this available in mouse over
          Map<String, String> properties = node.getProperties();
          label = new LabelData(id, id + "-" + typeName, children, properties);
          break;
        }
    }
    for (ConvertToRel child : node.getChildren()) {
      id++;
      children.add(writeNodeLabel(child));
    }
    return label;
  }
}
