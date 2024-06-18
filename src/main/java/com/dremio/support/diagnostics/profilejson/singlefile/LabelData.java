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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LabelData {
  private final int id;
  private final String label;
  private final List<LabelData> children;
  private Map<String, String> customData;

  /**
   * LabelData is the primary entry point to get the graph of connections and click events used for
   * mermaid
   *
   * @param id unique id for the label
   * @param label label text
   * @param children children of the label, so we can find dependencies
   * @param customData mouse over data for this particular label
   */
  public LabelData(int id, String label, List<LabelData> children, Map<String, String> customData) {
    this.id = id;
    this.label = label;
    this.children = children;
    this.customData = customData;
  }

  /**
   * getter for the id of the label helps with differentiating between things with the same name and
   * values and should always be unique
   *
   * @return id of the label
   */
  int getId() {
    return id;
  }

  /**
   * getting for the text for the label
   *
   * @return text for the label
   */
  String getLabel() {
    return label;
  }

  /**
   * getting for the children for this label which will result in connections in the UI
   *
   * @return children of this label
   */
  List<LabelData> getChildren() {
    return children;
  }

  /**
   * raw values as parsed from the convert to res text
   *
   * @return keys and values for the custom data
   */
  Map<String, String> getCustomData() {
    return customData;
  }

  /**
   * all the custom data for the label, this is visible in mouse over
   *
   * @return all of the custom data click/mouse over events used for mermaid
   */
  public String[] getAllCustomData() {
    return getCustomDataString(this).toArray(new String[0]);
  }

  /**
   * get custom data list from a specified LabelData
   *
   * @param data label to print javascript mouse over data for
   * @return list of strings that are click events for this label and all of it's children
   */
  static List<String> getCustomDataString(LabelData data) {
    List<String> labels = new ArrayList<>();
    String label = data.getLabel();
    Map<String, String> customData = data.getCustomData();
    Stream<String> customDataStream =
        customData.entrySet().stream()
            .filter(x -> !x.getKey().equals("columns"))
            .map(x -> String.format("%s=[%s]", x.getKey(), x.getValue()));
    String customDataString = String.join(", ", customDataStream.toArray(String[]::new));
    if (customDataString.length() > 0) {
      labels.add(
          String.format("click %s callback \"%s\";", label, customDataString.replace("\"", "'")));
    }
    for (LabelData l : data.getChildren()) {
      List<String> childLabel = getCustomDataString(l);
      labels.addAll(childLabel);
    }
    return labels;
  }

  /**
   * gets all the link text for the node, provides the text to draw a link between nodes
   *
   * @return get all link text
   */
  public String[] getLinks() {
    return getLink(this).toArray(new String[0]);
  }

  /**
   * get a mermaid links for each child
   *
   * @param data the label data to format
   * @return returns a list of mermaid links for each child in the label data
   */
  static List<String> getLink(LabelData data) {
    List<String> labels = new ArrayList<>();
    String label = data.getLabel();
    for (LabelData l : data.getChildren()) {
      String childLabel = l.getLabel();
      labels.add(String.format("%s-->%s;", label, childLabel));
      labels.addAll(getLink(l));
    }
    return labels;
  }
}
