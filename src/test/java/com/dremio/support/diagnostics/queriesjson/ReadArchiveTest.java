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

import com.dremio.support.diagnostics.queriesjson.filters.DateRangeQueryFilter;
import com.dremio.support.diagnostics.queriesjson.reporters.QueryReporter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class ReadArchiveTest {

  @Test
  public void testReadTarGzWithUnzippedFiles()
      throws IOException, InterruptedException, ExecutionException {
    var file = ReadArchiveTest.class.getResource("/queries.json.tgz");
    ReadArchive readArchive =
        new ReadArchive(new DateRangeQueryFilter(0, Instant.now().toEpochMilli()));
    var counter =
        new QueryReporter() {
          private long count;

          @Override
          public synchronized void parseRow(Query q) {
            count++;
          }

          public long getCount() {
            return count;
          }
        };
    var reporters = new ArrayList<QueryReporter>();
    reporters.add(counter);
    readArchive.readTarGz(file.getFile(), reporters, 2);
    assertEquals(11, counter.getCount());
  }
}
