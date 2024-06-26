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
