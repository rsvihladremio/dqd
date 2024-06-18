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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConvertToRelGraphParserTest {
  private ConvertToRel root;
  private Collection<ConvertToRel> children;
  private static final String grandParent = "NodeABC";
  private static final String firstChild = "ChildNodeABC";
  private static final String secondChild = "2ndChildNodeABC";
  private static final String grandChild = "GrandChildNodeABC";

  @BeforeEach
  void setup() {
    final List<ConvertToRelNode> nodes = new ArrayList<>();
    nodes.add(new ConvertToRelNode(1, grandParent, new HashMap<>(), 0));
    nodes.add(new ConvertToRelNode(2, firstChild, new HashMap<>(), 2));
    nodes.add(new ConvertToRelNode(3, grandChild, new HashMap<>(), 4));
    nodes.add(new ConvertToRelNode(4, secondChild, new HashMap<>(), 2));

    final ConvertToRelGraph graph = new ConvertToRelGraph(nodes);
    root = graph.getConvertToRelTree();
    children = root.getChildren();
  }

  @Test
  void testParentIsFirst() {
    String rootTypeName = root.getTypeName();
    assertEquals(grandParent, rootTypeName, "not the expected root");
  }

  @Test
  void testThereAre2Children() {
    assertEquals(2, children.size(), "not the expected number of children");
  }

  @Test
  void testFirstChildIsBelowParent() {
    ConvertToRel firstChildRel =
        children.stream().filter(x -> firstChild.equals(x.getTypeName())).findFirst().orElse(null);
    assertEquals(firstChild, firstChildRel.getTypeName(), "not the expected child");
  }

  @Test
  void testFirstChildHasCorrectGrandChildMapped() {
    ConvertToRel firstChildRel =
        children.stream().filter(x -> firstChild.equals(x.getTypeName())).findFirst().orElse(null);
    assertNotNull(firstChildRel);
    ConvertToRel convertToRel = firstChildRel.getChildren().stream().findFirst().get();
    assertEquals(grandChild, convertToRel.getTypeName(), "not the expected grandchild");
  }

  @Test
  void testFirstChildHas1Child() {
    ConvertToRel firstChildRel =
        children.stream().filter(x -> firstChild.equals(x.getTypeName())).findFirst().orElse(null);
    assertNotNull(firstChildRel);
    assertEquals(1, firstChildRel.getChildren().size(), "only supposed to be 1 grandchild");
  }

  @Test
  void test2ndChildIsBelowParent() {
    ConvertToRel secondChildRel =
        children.stream().filter(x -> secondChild.equals(x.getTypeName())).findFirst().orElse(null);
    assertEquals(secondChild, secondChildRel.getTypeName(), "not the expected child");
  }

  @Test
  void testSecondChildHas0Children() {
    ConvertToRel secondChildRel =
        children.stream().filter(x -> secondChild.equals(x.getTypeName())).findFirst().orElse(null);
    assertNotNull(secondChildRel);
    assertEquals(
        0, secondChildRel.getChildren().size(), "there are supposed to be no grandchildren");
  }

  static class BrotherConvertToRel {
    private List<ConvertToRelNode> sameTypes;
    private final String nodeType = "nodeabc";
    private Map<String, String> props;
    private ConvertToRelNode grandParent;
    private ConvertToRelNode parent;
    private ConvertToRelNode grandChild;
    private ConvertToRelNode uncle;
    private ConvertToRel root;

    @BeforeEach
    void before() {
      props = new HashMap<>();
      sameTypes = new ArrayList<>();
      grandParent = new ConvertToRelNode(1, nodeType, props, 0);
      sameTypes.add(grandParent);
      parent = new ConvertToRelNode(2, nodeType, props, 2);
      sameTypes.add(parent);
      grandChild = new ConvertToRelNode(3, nodeType, props, 4);
      sameTypes.add(grandChild);
      uncle = new ConvertToRelNode(4, nodeType, props, 2);
      sameTypes.add(uncle);
      final ConvertToRelGraph graph = new ConvertToRelGraph(sameTypes);
      root = graph.getConvertToRelTree();
    }

    @Test
    void testGrandParent() {
      assertEquals(2, root.getChildren().size(), "should have 2 children");
    }

    @Test
    void testParent() {
      assertEquals(
          1,
          root.getChildren().stream().findFirst().get().getChildren().size(),
          "should have 1 child");
    }

    @Test
    void testGrandChild() {
      assertEquals(
          0,
          root.getChildren().stream().findFirst().get().getChildren().stream()
              .findFirst()
              .get()
              .getChildren()
              .size(),
          "should have no children");
    }

    @Test
    void testUncle() {
      assertEquals(
          0,
          root.getChildren().stream().skip(1).findFirst().get().getChildren().size(),
          "should have no children");
    }
  }
}
