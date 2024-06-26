package com.dremio.support.diagnostics.queriesjson.filters;

import com.dremio.support.diagnostics.queriesjson.Query;

public interface QueryFilter {
    boolean isValid(Query q);
}
