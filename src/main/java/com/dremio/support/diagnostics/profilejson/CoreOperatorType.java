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
package com.dremio.support.diagnostics.profilejson;

public enum CoreOperatorType {
  SINGLE_SENDER, // => 0,
  BROADCAST_SENDER, // => 1,
  FILTER, // => 2,
  HASH_AGGREGATE, // => 3,
  HASH_JOIN, // => 4,
  MERGE_JOIN, // => 5,
  HASH_PARTITION_SENDER, // => 6,
  LIMIT, // => 7,
  MERGING_RECEIVER, // => 8,
  ORDERED_PARTITION_SENDER, // => 9,
  PROJECT, // => 10,
  UNORDERED_RECEIVER, // => 11,
  RANGE_SENDER, // => 12,
  SCREEN, // => 13,
  SELECTION_VECTOR_REMOVER, // => 14,
  STREAMING_AGGREGATE, // => 15,
  TOP_N_SORT, // => 16,
  EXTERNAL_SORT, // => 17,
  TRACE, // => 18,
  UNION, // => 19,
  OLD_SORT, // => 20,
  PARQUET_ROW_GROUP_SCAN, // => 21,
  HIVE_SUB_SCAN, // => 22,
  SYSTEM_TABLE_SCAN, // => 23,
  MOCK_SUB_SCAN, // => 24,
  PARQUET_WRITER, // => 25,
  DIRECT_SUB_SCAN, // => 26,
  TEXT_WRITER, // => 27,
  TEXT_SUB_SCAN, // => 28,
  JSON_SUB_SCAN, // => 29,
  INFO_SCHEMA_SUB_SCAN, // => 30,
  COMPLEX_TO_JSON, // => 31,
  PRODUCER_CONSUMER, // => 32,
  HBASE_SUB_SCAN, // => 33,
  WINDOW, // => 34,
  NESTED_LOOP_JOIN, // => 35,
  AVRO_SUB_SCAN, // => 36,
  MONGO_SUB_SCAN, // => 37,
  ELASTICSEARCH_SUB_SCAN, // => 38,
  ELASTICSEARCH_AGGREGATOR_SUB_SCAN, // => 39,
  FLATTEN, // => 40,
  EXCEL_SUB_SCAN, // => 41,
  ARROW_SUB_SCAN, // => 42,
  ARROW_WRITER, // => 43,
  JSON_WRITER, // => 44,
  VALUES_READER, // => 45,
  CONVERT_FROM_JSON, // => 46,
  JDBC_SUB_SCAN, // => 47,
  DICTIONARY_LOOKUP, // => 48,
  WRITER_COMMITTER, // => 49,
  ROUND_ROBIN_SENDER, // => 50,
  BOOST_PARQUET, // => 51,
  ICEBERG_SUB_SCAN, // => 52,
  TABLE_FUNCTION, // => 53,
  DELTALAKE_SUB_SCAN, // => 54,
  DIR_LISTING_SUB_SCAN, // => 55,
  ICEBERG_WRITER_COMMITTER, // => 56,
  GRPC_WRITER, // => 57,
  MANIFEST_WRITER, // => 58,
  FLIGHT_SUB_SCAN, // => 59,
  BRIDGE_FILE_WRITER_SENDER, // => 60,
  BRIDGE_FILE_READER_RECEIVER, // => 61,
  BRIDGE_FILE_READER, // => 62,
  ICEBERG_MANIFEST_WRITER, // => 63,
  ICEBERG_METADATA_FUNCTIONS_READER, // => 64,
  ICEBERG_SNAPSHOTS_SUB_SCAN, // => 65,
  NESSIE_COMMITS_SUB_SCAN, // => 66,
  SMALL_FILE_COMBINATION_WRITER, // = >67,
}
