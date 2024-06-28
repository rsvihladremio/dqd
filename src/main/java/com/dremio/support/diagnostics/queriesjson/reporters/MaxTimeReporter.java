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

public class MaxTimeReporter implements QueryReporter {
  private final Map<Long, Long> pending = new HashMap<>();

  public Map<Long, Long> getPending() {
    return pending;
  }

  private final Map<Long, Long> metadata = new HashMap<>();

  public Map<Long, Long> getMetadata() {
    return metadata;
  }

  private final Map<Long, Long> queued = new HashMap<>();

  public Map<Long, Long> getQueued() {
    return queued;
  }

  private final Map<Long, Long> planning = new HashMap<>();

  public Map<Long, Long> getPlanning() {
    return planning;
  }

  private final Map<Long, Long> maxPool = new HashMap<>();

  public Map<Long, Long> getMaxPool() {
    return maxPool;
  }

  private final Lock pendingLock = new ReentrantLock();
  private final Lock metadataLock = new ReentrantLock();
  private final Lock queuedLock = new ReentrantLock();
  private final Lock planningLock = new ReentrantLock();
  private final Lock maxPoolLock = new ReentrantLock();

  private final long window;

  public MaxTimeReporter(final long window) {
    this.window = window;
  }

  private void setMax(long measure, Long start, Map<Long, Long> values, Lock lock) {
    lock.lock();
    try {
      if (values.containsKey(start)) {
        long i = values.get(start);
        values.put(start, Math.max(measure, i));
      } else {
        values.put(start, measure);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void parseRow(Query q) {
    long start = TimeUtils.truncateEpoch(q.getStart(), this.window);
    // we add a second to make sure we count the last bucket. this value when
    // reached will stop the
    // counting and
    // therefore the finish will not added to the maps
    long finish = TimeUtils.truncateEpoch(q.getFinish(), this.window) + this.window;
    while (start < finish) {
      setMax(q.getPendingTime(), start, pending, pendingLock);
      setMax(q.getNormalizedMetadataRetrieval(), start, metadata, metadataLock);
      setMax(q.getQueuedTime(), start, queued, queuedLock);
      setMax(q.getPlanningTime(), start, planning, planningLock);
      setMax(q.getPoolWaitTime(), start, maxPool, maxPoolLock);
      start += this.window;
    }
  }
}
