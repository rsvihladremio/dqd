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
package com.dremio.support.diagnostics.shared;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TextRowBaseReportTest {

  private static String text;

  @BeforeAll
  public static void setup() {
    String[][] rows =
        new String[][] {
          new String[] {"my title1", "29991", "10/10/10", "this is also a desc"},
          new String[] {"my title2", "19991", "10/30/11", "this is my desc"},
        };
    String[] header =
        new String[] {
          "title", "value", "date", "desc",
        };
    MockTextRowReport reporter = new MockTextRowReport(rows, header);
    text = reporter.getText();
  }

  @Test
  void testTitleIsNotAddedToText() {
    MockTextRowReport report = new MockTextRowReport(new String[][] {}, new String[] {});
    assertThat(text).doesNotContain(report.getTitle());
  }

  @Nested
  class TestFindRow1InTextReport {

    @Test
    void testContainsTitle() {
      assertThat(text).contains("my title1 ");
    }

    @Test
    void testContainsValue() {
      assertThat(text).contains(" 29991 ");
    }

    @Test
    void testContainsDate() {
      assertThat(text).contains(" 10/10/10 ");
    }

    @Test
    void testContainsDescription() {
      assertThat(text).contains(" this is also a desc ");
    }
  }

  @Nested
  class TestFindRow2InTextReport {

    @Test
    void testContainsTitle() {
      assertThat(text).contains(" my title2 ");
    }

    @Test
    void testContainsValue() {
      assertThat(text).contains(" 19991 ");
    }

    @Test
    void testContainsDate() {
      assertThat(text).contains(" 10/30/11 ");
    }

    @Test
    void testContainsDescription() {
      assertThat(text).contains(" this is my desc ");
    }
  }

  @Nested
  class TestFindHeaderInTextReport {
    @Test
    void testContainsTitle() {
      assertThat(text).contains(" title  ");
    }

    @Test
    void testContainsValue() {
      assertThat(text).contains(" value ");
    }

    @Test
    void testContainsDate() {
      assertThat(text).contains(" date  ");
    }

    @Test
    void testContainsDescription() {
      assertThat(text).contains(" desc  ");
    }
  }
}

class MockTextRowReport extends TextRowBaseReport {
  public MockTextRowReport(String[][] rows, String... header) {
    super(rows, header);
  }

  @Override
  public String getTitle() {
    return "my test title";
  }
}
