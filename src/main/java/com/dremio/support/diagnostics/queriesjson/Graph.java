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
package com.dremio.support.diagnostics.queriesjson;

import java.util.List;
import java.util.function.Predicate;

/** interface to convert a list of queries into a series of data points ready to be graphed */
public interface Graph {

  long getBucketSizeMillis();

  /**
   * gets a filtered down list of second long duration buckets
   *
   * @param queries queries to analyze
   * @param filter filter to remove certain queries from the dataset
   * @param mapper aggregator responsible for performing calculations on the data
   * @return results of the analysis
   */
  DataPoints getFilteredBuckets(List<Query> queries, Predicate<Query> filter, Aggregator mapper);
}
