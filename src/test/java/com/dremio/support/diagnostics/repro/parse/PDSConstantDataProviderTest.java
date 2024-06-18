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
package com.dremio.support.diagnostics.repro.parse;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class PDSConstantDataProviderTest {
  private final PDSConstantDataProvider provider = new PDSConstantDataProvider();

  @Test
  void testBoolean() {
    assertThat(provider.getBoolean()).isTrue();
  }

  @Test
  void testString() {
    assertThat(provider.getString()).isEqualTo("Hello world!");
  }

  @Test
  void testLocalDate() {
    assertThat(String.valueOf(provider.getLocalDate())).isEqualTo("2018-09-14");
  }

  @Test
  void testInstant() {
    assertThat(String.valueOf(provider.getInstant())).isEqualTo("2022-09-15T00:00:00Z");
  }

  @Test
  void testDouble() {
    assertThat(String.valueOf(provider.getDouble())).isEqualTo("1.0");
  }

  @Test
  void testFloat() {
    assertThat(String.valueOf(provider.getFloat())).isEqualTo("1.0");
  }

  @Test
  void testInt() {
    assertThat(String.valueOf(provider.getInt())).isEqualTo("1");
  }

  @Test
  void testLong() {
    assertThat(String.valueOf(provider.getLong())).isEqualTo("1");
  }

  @Test
  void testTime() {
    assertThat(String.valueOf(provider.getTime())).isEqualTo("00:00");
  }

  @Test
  void testList() {
    final List<String> result = provider.getList();
    final String firstElement = result.get(0);
    assertThat(firstElement).isEqualTo("Hello world!");
  }

  @Test
  void testInterval() {
    assertThat(provider.getInterval()).isEqualTo("INTERVAL '1' DAY");
  }
}
