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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class QueriesJsonTextReportTest {

  @Test
  public void testWhenThereAreNoQueries() {
    List<Query> list = new ArrayList<Query>();
    QueriesJsonTextReport console = new QueriesJsonTextReport(list.stream());
    assertThat(console.getText().trim()).isEqualTo("no queries found");
  }

  @Test
  public void testWithOneQuery() {
    List<Query> list = new ArrayList<>();
    Query query1 = new Query();
    query1.setStart(1L);
    query1.setFinish(100L);
    query1.setQueryText("select * from world");
    list.add(query1);
    QueriesJsonTextReport console = new QueriesJsonTextReport(list.stream());
    String text = console.getText();
    assertThat(text.trim()).contains("select * from world");
  }

  @Test
  public void testWithSeveralQueries() {
    List<Query> list = new ArrayList<Query>();
    Query query1 = new Query();
    query1.setStart(1L);
    query1.setFinish(100L);
    query1.setQueryText("SELECT * FROM tester WHERE status=2");
    list.add(query1);
    Query query2 = new Query();
    query2.setStart(2L);
    query2.setFinish(100L);
    query2.setMemoryAllocated(2048);
    query2.setQueryText("SELECT * FROM tester WHERE status=19");
    list.add(query2);
    QueriesJsonTextReport console = new QueriesJsonTextReport(list.stream());
    String text = console.getText();
    assertThat(text.trim()).contains("max memory allocated");
    assertThat(text.trim()).contains("2.00 kb");
  }
}
