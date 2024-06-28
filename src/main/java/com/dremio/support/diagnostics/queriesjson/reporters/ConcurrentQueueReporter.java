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

public class ConcurrentQueueReporter implements QueryReporter {
  private Map<String, Map<Long, Long>> queueBucketCounts = new HashMap<>();

  public Map<String, Map<Long, Long>> getQueueBucketCounts() {
    return queueBucketCounts;
  }

  private final Lock lock = new ReentrantLock();
  private final long window;

  public ConcurrentQueueReporter(final long window) {
    this.window = window;
  }

  @Override
  public void parseRow(Query q) {
    var start = TimeUtils.truncateEpoch(q.getStart(), this.window);
    // we add a second to make sure we count the last bucket. this value when
    // reached will stop the
    // counting and
    // therefore the finish will not added to the counts map
    var finish = TimeUtils.truncateEpoch(q.getFinish(), this.window) + this.window;
    var queueName = q.getQueueName();
    while (start < finish) {
      lock.lock();
      try {
        if (queueBucketCounts.containsKey(queueName)) {
          // nested map sorry this is hard to read, but I'm checking to see if the bucket
          // is
          // in the queueBucketCounts
          var perQueueCounts = queueBucketCounts.get(queueName);
          if (perQueueCounts.containsKey(start)) {
            // ok so now we have a bucket count. let's increment it by one
            var count = perQueueCounts.get(start);
            perQueueCounts.put(start, count + 1L);
          } else {
            // there is no previous count for this bucket, go ahead and set it to 1
            perQueueCounts.put(start, 1L);
          }
          // ok so now that we've done everything with the perQueueCounts that we need to
          // go ahead and ovewrite the previous value
          queueBucketCounts.put(queueName, perQueueCounts);
        } else {
          final Map<Long, Long> values = new HashMap<>();
          values.put(start, 1L);
          queueBucketCounts.put(queueName, values);
        }
        // we are now safe to unlock
      } finally {
        lock.unlock();
      }
      start += this.window;
    }
  }
}
