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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ScanCrel extends ConvertToRel {

  private final long splits;
  private final String[] columns;
  private final String table;
  private final long snapshot;

  public ScanCrel(List<ConvertToRel> children, Map<String, String> properties) {
    super(ScanCrel.class.getSimpleName(), children, properties);
    this.splits = getLongProp("splits");
    this.columns = getStringProp("columns").split(", ");
    this.table = getStringProp("table");
    this.snapshot = getLongProp("snapshot");
  }

  public long getSplits() {
    return this.splits;
  }

  public String[] getColumns() {
    return this.columns;
  }

  public String getTable() {
    return this.table;
  }

  public long getSnapshot() {
    return this.snapshot;
  }

  @Override
  public String toString() {
    return "ScanCrel [splits="
        + splits
        + ", columns="
        + Arrays.toString(columns)
        + ", table="
        + table
        + ", snapshot="
        + snapshot
        + "]";
  }
}
