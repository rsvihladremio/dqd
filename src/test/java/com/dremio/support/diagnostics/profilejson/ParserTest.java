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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.support.diagnostics.FileTestHelpers;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ParserTest {

  private static ProfileJSON json;

  @BeforeAll
  static void initAll() throws IOException {
    ProfileJSONParser parser = new ProfileJSONParser();
    json = parser.parseFile(FileTestHelpers.getTestProfile1().stream());
  }

  @Test
  void testQuery() {

    assertThat(json.getQuery())
        .isEqualTo(
            "SELECT \"SF weather 2018-2019.csv\".\"DATE\" AS \"DATE\", ELEVATION, MAX(TAVG) AS"
                + " Maximum_TAVG, MIN(TMIN) AS Minimum_TMIN, MAX(TMAX) AS Maximum_TMAX\n"
                + "FROM Samples.\"samples.dremio.com\".\"SF weather 2018-2019.csv\"\n"
                + "GROUP BY \"SF weather 2018-2019.csv\".\"DATE\", ELEVATION");
  }

  @Test
  void testPlan() {
    assertThat(json.getPlan())
        .isEqualTo(
            "00-00    Screen : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT Records,"
                + " VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT"
                + " FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema,"
                + " VARBINARY(65536) ARRAY PartitionData): rowcount = 1004.7199999999997,"
                + " cumulative cost = {20988.072000000007 rows, 409286.3672967999 cpu, 132200.0 io,"
                + " 132200.2 network, 139603.2 memory}, id = 1452\n"
                + "00-01      Project(Fragment=[$0], Records=[$1], Path=[$2], Metadata=[$3],"
                + " Partition=[$4], FileSize=[$5], IcebergMetadata=[$6], fileschema=[$7],"
                + " PartitionData=[$8]) : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT"
                + " Records, VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition,"
                + " BIGINT FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema,"
                + " VARBINARY(65536) ARRAY PartitionData): rowcount = 1004.7199999999997,"
                + " cumulative cost = {20887.600000000006 rows, 409185.8952967999 cpu, 132200.0 io,"
                + " 132200.0 network, 139603.2 memory}, id = 1451\n"
                + "00-02       "
                + " WriterCommitter(final=[/home/ryansvihla/Downloads/dremio-enterprise-21.6.0-202209301921120677-ad35777b/data/pdfs/results/1cb80d46-af06-c937-6391-32ad4fae4f00])"
                + " : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT Records, VARCHAR(65536)"
                + " Path, VARBINARY(65536) Metadata, INTEGER Partition, BIGINT FileSize,"
                + " VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema, VARBINARY(65536)"
                + " ARRAY PartitionData): rowcount = 1004.7199999999997, cumulative cost ="
                + " {19882.880000000005 rows, 409185.8048719999 cpu, 132200.0 io, 132200.0 network,"
                + " 139603.2 memory}, id = 1450\n"
                + "00-03          Writer : rowType = RecordType(VARCHAR(65536) Fragment, BIGINT"
                + " Records, VARCHAR(65536) Path, VARBINARY(65536) Metadata, INTEGER Partition,"
                + " BIGINT FileSize, VARBINARY(65536) IcebergMetadata, VARBINARY(65536) fileschema,"
                + " VARBINARY(65536) ARRAY PartitionData): rowcount = 1004.7199999999997,"
                + " cumulative cost = {18878.160000000003 rows, 408181.0848719999 cpu, 132200.0 io,"
                + " 132200.0 network, 139603.2 memory}, id = 1449\n"
                + "00-04            Project(DATE=[$0], ELEVATION=[$1], Maximum_TAVG=[$2],"
                + " Minimum_TMIN=[$3], Maximum_TMAX=[$4]) : rowType = RecordType(VARCHAR(65536)"
                + " DATE, VARCHAR(65536) ELEVATION, VARCHAR(65536) Maximum_TAVG, VARCHAR(65536)"
                + " Minimum_TMIN, VARCHAR(65536) Maximum_TMAX): rowcount = 1004.7199999999997,"
                + " cumulative cost = {17873.440000000002 rows, 407176.36487199995 cpu, 132200.0"
                + " io, 132200.0 network, 139603.2 memory}, id = 1448\n"
                + "00-05              Project(DATE=[$0], ELEVATION=[$1], Maximum_TAVG=[$2],"
                + " Minimum_TMIN=[$3], Maximum_TMAX=[$4]) : rowType = RecordType(VARCHAR(65536)"
                + " DATE, VARCHAR(65536) ELEVATION, VARCHAR(65536) Maximum_TAVG, VARCHAR(65536)"
                + " Minimum_TMIN, VARCHAR(65536) Maximum_TMAX): rowcount = 1004.7199999999997,"
                + " cumulative cost = {16868.72 rows, 407176.31463599997 cpu, 132200.0 io, 132200.0"
                + " network, 139603.2 memory}, id = 1447\n"
                + "00-06                HashAgg(group=[{0, 1}], Maximum_TAVG=[MAX($2)],"
                + " Minimum_TMIN=[MIN($3)], Maximum_TMAX=[MAX($4)]) : rowType ="
                + " RecordType(VARCHAR(65536) DATE, VARCHAR(65536) ELEVATION, VARCHAR(65536)"
                + " Maximum_TAVG, VARCHAR(65536) Minimum_TMIN, VARCHAR(65536) Maximum_TMAX):"
                + " rowcount = 1004.7199999999997, cumulative cost = {15864.0 rows, 407176.2644"
                + " cpu, 132200.0 io, 132200.0 network, 139603.2 memory}, id = 1446\n"
                + "00-07                  Project(DATE=[$1], ELEVATION=[$0], TAVG=[$2], TMIN=[$4],"
                + " TMAX=[$3]) : rowType = RecordType(VARCHAR(65536) DATE, VARCHAR(65536)"
                + " ELEVATION, VARCHAR(65536) TAVG, VARCHAR(65536) TMIN, VARCHAR(65536) TMAX):"
                + " rowcount = 5288.0, cumulative cost = {10576.0 rows, 132200.2644 cpu, 132200.0"
                + " io, 132200.0 network, 0.0 memory}, id = 1445\n"
                + "00-08                    EasyScan(table=[Samples.\"samples.dremio.com\".\"SF"
                + " weather 2018-2019.csv\"], columns=[`ELEVATION`, `DATE`, `TAVG`, `TMAX`,"
                + " `TMIN`], splits=[1]) : rowType = RecordType(VARCHAR(65536) ELEVATION,"
                + " VARCHAR(65536) DATE, VARCHAR(65536) TAVG, VARCHAR(65536) TMAX, VARCHAR(65536)"
                + " TMIN): rowcount = 5288.0, cumulative cost = {5288.0 rows, 132200.0 cpu,"
                + " 132200.0 io, 132200.0 network, 0.0 memory}, id = 1444\n");
  }
}
