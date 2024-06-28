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
package com.dremio.support.diagnostics.queriesjson.html;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Dates {

  public static class BucketIterator implements Iterator<Long> {

    private final Lock lock = new ReentrantLock();
    private final Long bucketSize;
    private final Long lastBucket;
    private Long currentBucket;

    public BucketIterator(long startEpochMillis, long endEpochMillis, long bucketSize) {
      this.bucketSize = bucketSize;
      this.currentBucket = startEpochMillis - (startEpochMillis % this.bucketSize);
      this.lastBucket = endEpochMillis - (endEpochMillis % this.bucketSize);
    }

    @Override
    public boolean hasNext() {
      this.lock.lock();
      try {
        return currentBucket < lastBucket;
      } finally {
        this.lock.unlock();
      }
    }

    @Override
    public Long next() {
      this.lock.lock();
      try {
        return this.currentBucket;
      } finally {
        this.currentBucket += this.bucketSize;
        this.lock.unlock();
      }
    }
  }

  public static String format(TemporalAccessor accessor) {
    return formatter.format(accessor);
  }

  private static final DateTimeFormatter formatter =
      DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.of("UTC"));
}
