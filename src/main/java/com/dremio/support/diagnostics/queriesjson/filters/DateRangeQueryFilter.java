package com.dremio.support.diagnostics.queriesjson.filters;

import com.dremio.support.diagnostics.queriesjson.Query;

public class DateRangeQueryFilter implements QueryFilter{

    private final long epochStart;
    private final long epochEnd;

    public DateRangeQueryFilter(long epochStart, long epochEnd){
        this.epochStart = epochStart;
        this.epochEnd = epochEnd;
    }

    @Override
    public boolean isValid(Query q) {
        return q.getStart() > epochEnd || q.getStart() < epochStart;
    }
}
