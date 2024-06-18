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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.junit.jupiter.api.Test;

class HumanTest {
  private String numberSeparator() {
    DecimalFormat formatter = (DecimalFormat) NumberFormat.getInstance();
    return String.valueOf(formatter.getDecimalFormatSymbols().getDecimalSeparator());
  }

  @Test
  void testGetHumanDurationFromMillisWithZero() {
    assertThat(Human.getHumanDurationFromMillis(0L)).isEqualTo("0 millis");
  }

  @Test
  void testGetHumanDurationFromMillisOneMS() {
    assertThat(Human.getHumanDurationFromMillis(1L)).isEqualTo("1 milli");
  }

  @Test
  void testGetHumanDurationFromMillisMoreThanOneMS() {
    assertThat(Human.getHumanDurationFromMillis(2L)).isEqualTo("2 millis");
  }

  @Test
  void testGetHumanDurationFromMillisToSecondsWithOneSecond() {
    assertThat(Human.getHumanDurationFromMillis(1000L)).isEqualTo("1 second");
  }

  @Test
  void testGetHumanDurationFromMillisToSecondsWithMoreThanOneSecond() {
    assertThat(Human.getHumanDurationFromMillis(2010L))
        .isEqualTo("2" + numberSeparator() + "01 seconds");
  }

  @Test
  void testGetHumanDurationFromMillisToMinutesOneMinute() {
    assertThat(Human.getHumanDurationFromMillis(60000L)).isEqualTo("1 minute");
  }

  @Test
  void testGetHumanDurationFromMillisToMinutesMoreThanOneMinute() {
    assertThat(Human.getHumanDurationFromMillis(90000L))
        .isEqualTo("1" + numberSeparator() + "50 minutes");
  }

  @Test
  void testGetHumanDurationFromMillisToHoursOneHour() {
    assertThat(Human.getHumanDurationFromMillis(60000L * 60)).isEqualTo("1 hour");
  }

  @Test
  void testGetHumanDurationFromMillisToHoursMoreThanOneHour() {
    assertThat(Human.getHumanDurationFromMillis(90000L * 60))
        .isEqualTo("1" + numberSeparator() + "50 hours");
  }

  @Test
  void testGetHumanDurationFromMillisToDaysOneDay() {
    assertThat(Human.getHumanDurationFromMillis(60000L * 60 * 24)).isEqualTo("1 day");
  }

  @Test
  void testGetHumanDurationFromMillisToDaysMoreThanOneDay() {
    assertThat(Human.getHumanDurationFromMillis(90000L * 60 * 24))
        .isEqualTo("1" + numberSeparator() + "50 days");
  }
}
