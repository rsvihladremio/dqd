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

import java.util.HashMap;
import java.util.Map;

public class ConvertToRelParser {

  /**
   * Parses a line in the Convert To Rel plan phase (effectively a node of the ConvertToRel graph)
   * and converts it into a object with properties, this should allow us to do more intelligent
   * things with the plan data instead of just manaully reading a giant blob of text and allows us
   * to generate interesting graphs and recommendations, this is the most important line of Convert
   * To Rel parsing
   *
   * @param id id to set when creating the ConvertToRelNode
   * @param nodeText line (separted by \n characters) from the Convert To Rel plan phase
   * @return the line as represented by it's various components; name, properties, indention level
   *     in plan
   */
  public static ConvertToRelNode parseConvertToRelFormattedLine(
      final int id, final String nodeText) {
    // track parens to know when the line is done
    int openParens = 0;
    int closedParens = 0;
    // we track brackets to know when a property value has been parsed
    int openBrackets = 0;
    int closedBrackets = 0;
    // we track indent depth to know which node is a child or parent of another node
    int indentDepth = 0;
    // this is to know when we are parsing spaces to find the indent depth
    boolean parsedStartingSpaces = false;
    // the type of node we are parsing
    String nodeType = "";
    // when we have finished parsing the node type
    boolean nodeTypeFound = false;
    // properties of the node
    Map<String, String> properties = new HashMap<>();
    // we will toggle this repeatedly as we walk through the properties
    boolean propertyNameFound = false;
    // we are going to search for a property
    boolean startSearchingProperty = false;
    // current property name we are parsing. Is reset for each property found
    String currentPropertyName = "";
    // current property value we are parsing. Is reset for each property found
    String currentPropertyValue = "";

    for (int i = 0; i < nodeText.length(); i++) {
      final char c = nodeText.charAt(i);

      // Phase 1: count the number of spaces the line starts with to find child and parent
      // relationships between nodes
      if (!parsedStartingSpaces) {
        if (c == ' ') {
          indentDepth += 1;
          // process no further
          continue;
        }
        // when able to get past the first guard we know we are done with this step
        parsedStartingSpaces = true;
      }
      // Phase 2: find node type
      if (!nodeTypeFound) {

        // ends phase 2 as soon as we find the opening (
        if (c == '(') {
          openParens +=
              1; // we store this for later calculations to know when we are done processing
          nodeTypeFound = true;
          // we assume there is a property name right away to start searching
          startSearchingProperty = true;
          // we can skip this now that we have finished the entity name. we can safely enter phase 3
          continue;
        }
        // so we have not found the entity name it is safe to add to it
        nodeType += c;
      } else {
        // phase 4: find the end of the file, this is before phase 3 in the code to guard against
        // adding more when the file is effectively already done.
        // Therefore, we need to count all open and close parens
        if (c == ')') {
          closedParens += 1;
        }
        if (c == '(') {
          openParens += 1;
        }
        // work complete all is done
        if (openParens == closedParens) {
          // go ahead and exit the loop, we assuming currentPropertyName and currentPropertyValue
          // have already been added to the properties map in phase 3.
          break;
        }
        // phase 4 END:

        // phase 3: search for properties
        // if closed brackets == open brackets then we need complete this property
        // this also means we need to always be counting the brackets
        if (c == ']') {
          closedBrackets += 1;
        }
        if (c == '[') {
          openBrackets += 1;
          // skip adding first bracket
          if (openBrackets == 1 && closedBrackets == 0) {
            continue;
          }
        }
        // we of course want to skip the "first run" check so that 0==0 does not match, so we add
        // closedBrackets > 0 to guard against this scenario.
        // all other cases we do want to execute this closing routing for a property.
        if (closedBrackets > 0 && closedBrackets == openBrackets) {
          // since we are not picky about what types of characters we stored, go ahead and trim
          // spaces out
          properties.put(currentPropertyName.trim(), currentPropertyValue.trim());
          // blank out properties now for the next property found
          currentPropertyName = "";
          currentPropertyValue = "";
          // blank out bracket count for next property
          openBrackets = 0;
          closedBrackets = 0;
          // set up the property name again
          propertyNameFound = false;
          // stop searching for the property until the ', ' combo is found
          startSearchingProperty = false;
          // nothing else to do move on
          continue;
        }
        if (!startSearchingProperty) {
          // when we enconter the space and we are not actively searching the property it is safe to
          // assume we can start again.
          // we do not want to capture this space however or so we can throw it away by using
          // continue
          if (c == ' ') {
            startSearchingProperty = true;
            continue;
          }
        } else {
          // next once we hit the first = we turn off current property name search and
          if (!propertyNameFound && c == '=') {
            propertyNameFound = true;
            continue;
          }
          // everything else..add to the currently property name or value depending on the mode we
          // are in
          if (propertyNameFound) {
            // since we have already parsed the property name it is assumed we are interested in
            // parsing property value
            currentPropertyValue += c;
          } else {
            // since we have not yet found the property name we need to continue adding to it
            currentPropertyName += c;
          }
        }
      }
    }
    return new ConvertToRelNode(id, nodeType, properties, indentDepth);
  }
}
