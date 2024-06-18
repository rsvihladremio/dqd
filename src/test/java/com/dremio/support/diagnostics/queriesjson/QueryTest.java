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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QueryTest {
  @Test
  void testGetMetadataWhenTimeIsEpoch() {
    Query query = new Query();
    query.setMetadataRetrievalTime(1600000000000L);
    query.setMetadataRetrieval(100L);
    assertEquals(
        100L,
        query.getNormalizedMetadataRetrieval(),
        "expected to get a value of 100 for metadata retrieval");
  }

  @Test
  void testGetMetadataWhenIsEpoch() {
    Query query = new Query();
    query.setMetadataRetrieval(1600000000000L);
    query.setMetadataRetrievalTime(100L);
    assertEquals(
        100L,
        query.getNormalizedMetadataRetrieval(),
        "expected to get a value of 100 for metadata retrieval");
  }

  @Test
  void testGetMetadataWhenNeitherIsEpoch() {
    Query query = new Query();
    query.setMetadataRetrieval(100L);
    query.setMetadataRetrievalTime(0L);
    assertEquals(
        100L,
        query.getNormalizedMetadataRetrieval(),
        "expected to get a value of 100 for metadata retrieval");
  }

  @Test
  void testGetMetadataWhenBothAre0() {
    Query query = new Query();
    query.setMetadataRetrieval(0L);
    query.setMetadataRetrievalTime(0L);
    assertEquals(
        0L,
        query.getNormalizedMetadataRetrieval(),
        "expected to get a value of 0 for metadata retrieval");
  }
}
