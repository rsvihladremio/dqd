package com.dremio.support.diagnostics.queriesjson.reporters;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.dremio.support.diagnostics.queriesjson.Query;

public class MaxMemoryQueriesReporter implements QueryReporter {

    private List<Query> top5 = new ArrayList<>();
    private final long limit;

    public MaxMemoryQueriesReporter(final long limit){
        this.limit = limit;
    }

    @Override
    public void parseRow(Query q) {
        this.top5.add(q);
        // need to make sure use an array list to make this writeable again since toList makes it immutable
        this.top5 = new ArrayList<>(this.top5.stream()
        .sorted(Comparator.comparingLong(Query::getMemoryAllocated).reversed())
        .limit(this.limit).toList());
    }
    
}
