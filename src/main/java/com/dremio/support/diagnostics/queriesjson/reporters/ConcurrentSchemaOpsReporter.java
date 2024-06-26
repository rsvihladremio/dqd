package com.dremio.support.diagnostics.queriesjson.reporters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.dremio.support.diagnostics.queriesjson.Query;
import com.dremio.support.diagnostics.shared.TimeUtils;

public class ConcurrentSchemaOpsReporter implements QueryReporter {
    private Map<Long, Long> buckets = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private final long window;

    public ConcurrentSchemaOpsReporter(long window) {
        this.window = window;
    }

    @Override
    public void parseRow(Query q) {
        if (q.getQueryText() != null
                && (q.getQueryText().startsWith("DROP")
                        || q.getQueryText().startsWith("CREATE")
                        || q.getQueryText().startsWith("REFRESH")
                        || q.getQueryText().startsWith("ALTER"))) {
            long start = TimeUtils.truncateEpoch(q.getStart(), this.window);
            // we add another interval to make sure we count the last bucket. this value
            // when reached will stop the
            // counting and
            // therefore the finish will not added to the counts map
            long finish = TimeUtils.truncateEpoch(q.getFinish(), this.window) + this.window;
            while (start != finish) {
                lock.lock();
                if (buckets.containsKey(start)) {
                    long i = buckets.get(start);
                    buckets.put(start, i++);
                } else {
                    buckets.put(start, 1L);
                }
                lock.unlock();
                start += this.window;
            }
        }
    }
}
