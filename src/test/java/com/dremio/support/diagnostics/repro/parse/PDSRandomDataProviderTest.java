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

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PDSRandomDataProviderTest {

  private PDSRandomDataProvider provider;

  @BeforeEach
  void setup() {
    TimeProvider time = Mockito.spy(TimeProvider.class);
    Mockito.when(time.getInstant()).thenReturn(Instant.ofEpochMilli(1667910975L * 1000));
    provider = new PDSRandomDataProvider(1L, time);
  }

  @Test
  void testValidateConstructor() {
    assertThat(new PDSRandomDataProvider().getString()).isNotNull();
  }

  @Test
  void testBoolean() {
    assertThat(provider.getBoolean()).isTrue();
  }

  @Test
  void testString() {
    assertThat(provider.getString()).isEqualTo("Shark-Monkey-Giraffe");
  }

  @Test
  void testLocalDate() {
    assertThat(String.valueOf(provider.getLocalDate())).isEqualTo("2022-08-16");
  }

  @Test
  void testInstant() {
    assertThat(String.valueOf(provider.getInstant())).isEqualTo("2022-08-16T14:26:00Z");
  }

  @Test
  void testDouble() {
    assertThat(String.valueOf(provider.getDouble())).isEqualTo("0.7308781907032909");
  }

  @Test
  void testFloat() {
    assertThat(String.valueOf(provider.getFloat())).isEqualTo("0.7308782");
  }

  @Test
  void testInt() {
    assertThat(String.valueOf(provider.getInt())).isEqualTo("548985");
  }

  @Test
  void testLong() {
    assertThat(String.valueOf(provider.getLong())).isEqualTo("4964420948893066023");
  }

  @Test
  void testTime() {
    assertThat(String.valueOf(provider.getTime())).isEqualTo("14:26");
  }

  @Test
  void testListCount() {
    assertThat(provider.getList().size()).isEqualTo(5);
  }

  @Test
  void testListElements() {
    assertThat(provider.getList().get(0)).isEqualTo("Monkey-Giraffe-Panda");
  }

  @Test
  void testInterval() {
    assertThat(provider.getInterval()).isEqualTo("INTERVAL '548985' DAY");
  }
}
