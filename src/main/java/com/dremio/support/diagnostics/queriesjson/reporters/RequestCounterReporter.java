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
  public Map<String, Long> getRequestCounterMap() {
    return requestCounterMap;
  }

  @Override
  public void parseRow(final Query q) {
    final String outcome = q.getOutcome();
    if (requestCounterMap.containsKey(outcome)) {
      final Long total = requestCounterMap.get(outcome);
      requestCounterMap.put(outcome, total + 1L);
    } else {
      requestCounterMap.put(outcome, 1L);
    }
  }
}
