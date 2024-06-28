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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentQueriesReporter implements QueryReporter {
  private final Map<Long, Long> counts = new HashMap<>();

  public Map<Long, Long> getCounts() {
    return counts;
  }

  private final long window;
  private final Lock lock = new ReentrantLock();

  public ConcurrentQueriesReporter(long window) {
    this.window = window;
  }

  @Override
  public void parseRow(Query q) {
    long start = TimeUtils.truncateEpoch(q.getStart(), this.window);
    // we add a second to make sure we count the last bucket. this value when reached will stop the
    // counting and
    // therefore the finish will not added to the counts map
    long finish = TimeUtils.truncateEpoch(q.getFinish(), this.window) + this.window;
    while (start < finish) {
      lock.lock();
      if (counts.containsKey(start)) {
        long i = counts.get(start);
        counts.put(start, i++);
      } else {
        counts.put(start, 1L);
      }
      lock.unlock();
      start += this.window;
    }
  }
}
