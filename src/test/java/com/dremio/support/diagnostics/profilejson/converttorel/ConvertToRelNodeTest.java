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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConvertToRelNodeTest {

  private static final String nodeType = "MyNode";
  private static final Map<String, String> props = Collections.singletonMap("myProp1", "abc");
  private static final int indentDepth = 4;
  private static final int id = 1;
  private ConvertToRelNode node1;

  @BeforeEach
  void setup() {
    node1 = new ConvertToRelNode(id, nodeType, props, indentDepth);
  }

  @Test
  void testGetId() {
    assertEquals(id, node1.getId(), "id passed to ctor did not equal getter");
  }

  @Test
  void testGetIndentDepth() {
    assertEquals(
        indentDepth, node1.getIndentDepth(), "indentDepth passed to ctor did not equal getter");
  }

  @Test
  void testGetNodeType() {
    assertEquals(nodeType, node1.getNodeType(), "nodeType passed to ctro did not equal getter");
  }

  @Test
  void testGetPropertyCount() {
    assertEquals(
        props.size(),
        node1.getProperties().size(),
        "property passed to ctro did not have same number of elements in the Getter");
  }

  @Test
  void testGetProperties() {
    final String propName = "myProp1";
    final String propValue = props.get(propName);
    final Map<String, String> node1Props = node1.getProperties();
    final String getterPropValue = node1Props.get(propName);
    assertEquals(
        propValue,
        getterPropValue,
        "properties passed to ctro lost it's entry by the we called the getProperties() method,"
            + " something is very wrong");
  }

  @Test
  void testToString() {
    assertEquals(
        "ConvertToRelNode [id=1, nodeType=MyNode, properties={myProp1=abc}, indentDepth=4]",
        node1.toString(),
        "the string generated is unexpected");
  }
}
