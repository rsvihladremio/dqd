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
package com.dremio.support.diagnostics.queriesjson.filters;

import com.dremio.support.diagnostics.queriesjson.Query;

public class DateRangeQueryFilter implements QueryFilter {

  private final long epochStart;
  private final long epochEnd;

  public DateRangeQueryFilter(long epochStart, long epochEnd) {
    this.epochStart = epochStart;
    this.epochEnd = epochEnd;
  }

  @Override
  public boolean isValid(Query q) {
    if (q.getStart() == 0) {
      return false;
    }
    if (q.getFinish() == 0) {
      return false;
    }
    return q.getStart() < epochEnd && q.getStart() > epochStart;
  }
}
