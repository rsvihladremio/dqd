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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.support.diagnostics.Strings;
import com.dremio.support.diagnostics.profilejson.singlefile.GraphWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GraphWriterTest {

  @Test
  void testOneDeepConvertToRel() {
    GraphWriter graphWriter = new GraphWriter();
    List<ConvertToRel> children = new ArrayList<>();
    Map<String, String> withFetch = new HashMap<>();
    withFetch.put("fetch", "1");
    ConvertToRel rel = new LogicalSort(children, withFetch);
    String mermaidText = graphWriter.writeMermaid(rel);
    int matches = Strings.countSubstring(mermaidText, "-->");
    assertEquals(0, matches, "we were expecting no messages");
  }

  @Test
  void testTwoDeepConvertToRel() {
    GraphWriter graphWriter = new GraphWriter();
    Map<String, String> withJoinType = new HashMap<>();
    withJoinType.put("joinType", "inner");
    ConvertToRel child = new LogicalJoin(new ArrayList<>(), withJoinType);
    List<ConvertToRel> children = new ArrayList<>();
    children.add(child);
    Map<String, String> withFetch = new HashMap<>();
    withFetch.put("fetch", "1");
    ConvertToRel rel = new LogicalSort(children, withFetch);
    String mermaidText = graphWriter.writeMermaid(rel);
    assertTrue(
        mermaidText.contains("0-LogicalSort\\nfetch:1-->1-LogicalJoin\\ntype:inner"),
        "expected text to contain logical sort connecting to logical join");
    int matches = Strings.countSubstring(mermaidText, "-->");
    assertEquals(1, matches, "we were expecting only one message");
  }

  @Test
  void testThreeLevelsWithMultipleChildrenPerNode() {
    GraphWriter graphWriter = new GraphWriter();
    Map<String, String> empty = new HashMap<>();
    List<ConvertToRel> parentChildren1 = new ArrayList<>();
    ConvertToRel parentChild1 = new ConvertToRel("Abc", new ArrayList<>(), empty);
    ConvertToRel parentChild2 = new ConvertToRel("Def", new ArrayList<>(), empty);
    parentChildren1.add(parentChild1);
    parentChildren1.add(parentChild2);
    ConvertToRel parentChild3 = new ConvertToRel("Xyz", new ArrayList<>(), empty);
    ConvertToRel parentChild4 = new ConvertToRel("Tuv", new ArrayList<>(), empty);
    List<ConvertToRel> parentChildren2 = new ArrayList<>();
    parentChildren2.add(parentChild3);
    parentChildren2.add(parentChild4);

    Map<String, String> withJoinType = new HashMap<>();
    withJoinType.put("joinType", "inner");
    ConvertToRel child1 = new LogicalJoin(parentChildren1, withJoinType);
    ConvertToRel child2 = new LogicalJoin(parentChildren2, withJoinType);
    List<ConvertToRel> children = new ArrayList<>();
    children.add(child1);
    children.add(child2);
    Map<String, String> withFetch = new HashMap<>();
    withFetch.put("fetch", "1");
    ConvertToRel rel = new LogicalSort(children, withFetch);
    final String mermaidText = graphWriter.writeMermaid(rel);
    assertAll(
        "all links are present",
        () ->
            assertTrue(
                mermaidText.contains("0-LogicalSort\\nfetch:1-->1-LogicalJoin\\ntype:inner"),
                "grand parent and child missing"),
        () ->
            assertTrue(
                mermaidText.contains("0-LogicalSort\\nfetch:1-->4-LogicalJoin\\ntype:inner"),
                "grand parent and child missing"),
        () ->
            assertTrue(
                mermaidText.contains("1-LogicalJoin\\ntype:inner-->2-Abc"),
                "logical join parent and child missing"),
        () ->
            assertTrue(
                mermaidText.contains("1-LogicalJoin\\ntype:inner-->3-Def"),
                "logical join parent and child missing"),
        () ->
            assertTrue(
                mermaidText.contains("4-LogicalJoin\\ntype:inner-->5-Xyz"), "grand child missing"),
        () ->
            assertTrue(
                mermaidText.contains("4-LogicalJoin\\ntype:inner-->6-Tuv"), "grand child missing"),
        () ->
            assertEquals(
                6,
                Strings.countSubstring(mermaidText, "-->"),
                "expecting 6 links but there were more or less"));
  }
}
