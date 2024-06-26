package com.dremio.support.diagnostics.queriesjson.reporters;

import com.dremio.support.diagnostics.queriesjson.Query;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryAllocatedReporter implements QueryReporter {

  private final Map<Long, Double> memoryCounter = new HashMap<>();
  private final Lock lock = new ReentrantLock();
  private final long bucketSize;

  public MemoryAllocatedReporter(final long bucketSize) {
    this.bucketSize = bucketSize;
  }

  private void update(Long bucket, Double value) {
    lock.lock();
    try {
      if (memoryCounter.containsKey(bucket)) {
        double prev = memoryCounter.get(bucket);
        memoryCounter.put(bucket, value + prev);
      } else {
        memoryCounter.put(bucket, value);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void parseRow(Query q) {
    final Long startBucket = q.getStart() - (q.getStart() % this.bucketSize);
    final Long finishBucket = q.getFinish() - (q.getFinish() % this.bucketSize);
    if (startBucket != finishBucket) {
      // here we have more than one bucket so we are going to split the memory allocation across all
      // buckets
      Long currentBucket = startBucket;
      Double perBucketAllocation =
          Double.valueOf(q.getMemoryAllocated()) / Double.valueOf(finishBucket - startBucket);
      while (currentBucket != finishBucket) {
        update(currentBucket, perBucketAllocation);
        currentBucket += bucketSize;
      }
    } else {
      // ok so we have just the start bucket so we can just fill it up usual
      update(startBucket, Double.valueOf(q.getMemoryAllocated()));
    }
  }
}
