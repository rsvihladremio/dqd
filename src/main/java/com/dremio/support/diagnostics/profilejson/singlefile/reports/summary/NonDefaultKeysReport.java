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
package com.dremio.support.diagnostics.profilejson.singlefile.reports.summary;

import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class NonDefaultKeysReport implements ProfileJSONReport {
  private static final Logger LOGGER = Logger.getLogger(NonDefaultKeysReport.class.getName());

  public static class SupportKey {
    private String kind;
    private String type;
    private String name;
    private Object value;

    public String getKind() {
      return kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Object getValue() {
      return value;
    }

    public void setValue(Object value) {
      this.value = value;
    }
  }

  @Override
  public String generateReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {
    List<Collection<HtmlTableDataColumn<Object, Object>>> rows = new ArrayList<>();
    for (final SupportKey key : NonDefaultKeysReport.getNonDefaultOptions(profileJson)) {
      rows.add(
          Arrays.asList(
              HtmlTableDataColumn.col(key.getName()),
              HtmlTableDataColumn.col(key.getKind()),
              HtmlTableDataColumn.col(key.getType()),
              HtmlTableDataColumn.col(key.getValue())));
    }
    HtmlTableBuilder builder = new HtmlTableBuilder();
    return builder.generateTable(
        "nonDefaultSupportKeys",
        "Non Default Support Keys",
        Arrays.asList("key", "kind", "type", "value"),
        rows);
  }

  public static List<SupportKey> getNonDefaultOptions(final ProfileJSON profileJson) {
    if (profileJson.getNonDefaultOptionsJSON() == null) {
      return new ArrayList<>();
    }
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> map;
    try {
      map =
          mapper.readValue(
              profileJson.getNonDefaultOptionsJSON().getBytes(StandardCharsets.UTF_8),
              new TypeReference<List<Map<String, Object>>>() {});
    } catch (final IOException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "unable to read json string %s due to error %s",
                  profileJson.getNonDefaultOptionsJSON(), e));
      throw new RuntimeException(e);
    }
    List<SupportKey> keys = new ArrayList<>();
    for (Map<String, Object> entry : map) {
      SupportKey key = new SupportKey();
      key.setKind(String.valueOf(entry.get("kind")));
      key.setType(String.valueOf(entry.get("type")));
      key.setName(String.valueOf(entry.get("name")));
      Object value = "";
      for (Map.Entry<String, Object> e : entry.entrySet()) {
        if (e.getKey().endsWith("_val")) {
          value = e.getValue();
          break;
        }
      }
      key.setValue(value);
      keys.add(key);
    }
    return keys;
  }
}
