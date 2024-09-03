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
package com.dremio.support.diagnostics.queriesjson.reporters;

import com.dremio.support.diagnostics.queriesjson.Query;
import com.dremio.support.diagnostics.shared.TimeUtils;
import java.util.HashMap;
import java.util.Map;

public class ConcurrentSchemaOpsReporter implements QueryReporter {
  private Map<Long, Long> buckets = new HashMap<>();

  public synchronized Map<Long, Long> getBuckets() {
    return buckets;
  }

  private final long window;

  public ConcurrentSchemaOpsReporter(long window) {
    this.window = window;
  }

  @Override
  public synchronized void parseRow(Query q) {
    if (q.getQueryText() != null
        && (q.getQueryText().startsWith("DROP")
            || q.getQueryText().startsWith("CREATE")
            || q.getQueryText().startsWith("REFRESH")
            || q.getQueryText().startsWith("ALTER"))) {
      long start = TimeUtils.truncateEpoch(q.getStart(), this.window);
      // we add another interval to make sure we count the last bucket. this value
      // when reached will stop the
      // counting and
      // therefore the finish will not added to the counts map
      long finish = TimeUtils.truncateEpoch(q.getFinish(), this.window) + this.window;
      while (start < finish) {
        if (buckets.containsKey(start)) {
          long i = buckets.get(start);
          buckets.put(start, i++);
        } else {
          buckets.put(start, 1L);
        }
        start += this.window;
      }
    }
  }
}
