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

public class MemoryAllocatedReporter implements QueryReporter {

  private final Map<Long, Double> memoryCounter = new HashMap<>();

  public synchronized Map<Long, Double> getMemoryCounter() {
    return memoryCounter;
  }

  private final long bucketSize;

  public MemoryAllocatedReporter(final long bucketSize) {
    this.bucketSize = bucketSize;
  }

  private void update(Long bucket, Double value) {
    if (memoryCounter.containsKey(bucket)) {
      double prev = memoryCounter.get(bucket);
      memoryCounter.put(bucket, value + prev);
    } else {
      memoryCounter.put(bucket, value);
    }
  }

  @Override
  public synchronized void parseRow(Query q) {
    Long startBucket = q.getStart() - (q.getStart() % this.bucketSize);
    final Long finishBucket = q.getFinish() - (q.getFinish() % this.bucketSize);
    if (startBucket < finishBucket) {
      // here we have more than one bucket so we are going to split the memory allocation across all
      // buckets
      Double perBucketAllocation =
          Double.valueOf(q.getMemoryAllocated()) / Double.valueOf(finishBucket - startBucket);
      while (startBucket < finishBucket) {
        update(startBucket, perBucketAllocation);
        startBucket += bucketSize;
      }
    } else {
      // ok so we have just the start bucket so we can just fill it up usual
      update(startBucket, Double.valueOf(q.getMemoryAllocated()));
    }
  }
}
