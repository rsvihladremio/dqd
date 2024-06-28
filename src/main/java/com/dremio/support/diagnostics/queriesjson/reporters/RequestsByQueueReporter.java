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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RequestsByQueueReporter implements QueryReporter {
  private final Map<String, Long> requestsByQueue = new HashMap<>();

  /** defensive copy of requestsByQueue */
  public synchronized Map<String, Long> getRequestsByQueue() {
    return Collections.unmodifiableMap(requestsByQueue);
  }

  @Override
  public synchronized void parseRow(Query q) {
    final String queueName = q.getQueueName();
    if (requestsByQueue.containsKey(queueName)) {
      Long count = requestsByQueue.get(queueName);
      requestsByQueue.put(queueName, count + 1L);
    } else {
      requestsByQueue.put(queueName, 1L);
    }
  }
}
