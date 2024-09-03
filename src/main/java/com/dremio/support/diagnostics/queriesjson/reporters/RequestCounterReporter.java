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
import java.util.HashMap;
import java.util.Map;

public class RequestCounterReporter implements QueryReporter {

  private final Map<String, Long> requestCounterMap = new HashMap<>();

  /**
   * getter for report
   *
   * @return map of request counts
   */
  public synchronized Map<String, Long> getRequestCounterMap() {
    return requestCounterMap;
  }

  @Override
  public synchronized void parseRow(final Query q) {
    final String outcome = q.getOutcome();
    if (requestCounterMap.containsKey(outcome)) {
      final Long total = requestCounterMap.get(outcome);
      requestCounterMap.put(outcome, total + 1L);
    } else {
      requestCounterMap.put(outcome, 1L);
    }
  }
}
