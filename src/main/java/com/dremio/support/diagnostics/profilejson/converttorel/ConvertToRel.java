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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConvertToRel {

  private final Collection<ConvertToRel> children;
  private final Map<String, String> properties;
  private final String typeName;

  /**
   * @param typeName type of ConvertToRel
   * @param children the children in the graph for this ConvertToRel
   * @param properties properties set
   */
  public ConvertToRel(
      final String typeName,
      final List<ConvertToRel> children,
      final Map<String, String> properties) {
    this.typeName = typeName;
    this.children = Collections.unmodifiableList(children);
    this.properties = Collections.unmodifiableMap(properties);
  }

  /**
   * The children of this particular node
   *
   * @return children of this ConvertToRel, ie the next items in the graph that send input into this
   *     ConvertToRel
   */
  public Collection<ConvertToRel> getChildren() {
    return children;
  }

  /**
   * The properties of the ConvertToRel
   *
   * @return properties of the given ConvertToRel, can be columns converted, type information,
   *     number of splits, etc
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * The type of the ConvertToRel
   *
   * @return the type of the ConvertToRel
   */
  public String getTypeName() {
    return typeName;
  }

  @Override
  public String toString() {
    return "ConvertToRel [children="
        + children
        + ", properties="
        + properties
        + ", nodeType="
        + typeName
        + "]";
  }

  /**
   * convenience method for getting string values from the property list
   *
   * @param propertyName the property to look for
   * @return property value as a string value
   */
  String getStringProp(final String propertyName) {
    return this.properties.get(propertyName);
  }

  /**
   * Convenience method for getting a long property by name
   *
   * @param propertyName the property name to look for
   * @return -1 is not present otherwise the property as a long
   */
  long getLongProp(final String propertyName) {
    final String raw = this.properties.get(propertyName);
    if (raw == null) {
      return -1L;
    }
    return Long.parseLong(this.properties.get(propertyName));
  }
}
