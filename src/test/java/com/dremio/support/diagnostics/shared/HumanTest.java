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

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    assertEquals(Human.getHumanDurationFromMillis(0L), "0 millis");
  }

  @Test
  void testGetHumanDurationFromMillisOneMS() {
    assertEquals(Human.getHumanDurationFromMillis(1L), "1 milli");
  }

  @Test
  void testGetHumanDurationFromMillisMoreThanOneMS() {
    assertEquals(Human.getHumanDurationFromMillis(2L), "2 millis");
  }

  @Test
  void testGetHumanDurationFromMillisToSecondsWithOneSecond() {
    assertEquals(Human.getHumanDurationFromMillis(1000L), "1 second");
  }

  @Test
  void testGetHumanDurationFromMillisToSecondsWithMoreThanOneSecond() {
    assertEquals(Human.getHumanDurationFromMillis(2010L), ("2" + numberSeparator() + "01 seconds"));
  }

  @Test
  void testGetHumanDurationFromMillisToMinutesOneMinute() {
    assertEquals(Human.getHumanDurationFromMillis(60000L), "1 minute");
  }

  @Test
  void testGetHumanDurationFromMillisToMinutesMoreThanOneMinute() {
    assertEquals(Human.getHumanDurationFromMillis(90000L), "1" + numberSeparator() + "50 minutes");
  }

  @Test
  void testGetHumanDurationFromMillisToHoursOneHour() {
    assertEquals(Human.getHumanDurationFromMillis(60000L * 60), "1 hour");
  }

  @Test
  void testGetHumanDurationFromMillisToHoursMoreThanOneHour() {
    assertEquals(Human.getHumanDurationFromMillis(90000L * 60), "1" + numberSeparator() + "50 hours");
  }

  @Test
  void testGetHumanDurationFromMillisToDaysOneDay() {
    assertEquals(Human.getHumanDurationFromMillis(60000L * 60 * 24), "1 day");
  }

  @Test
  void testGetHumanDurationFromMillisToDaysMoreThanOneDay() {
    var msg = Human.getHumanDurationFromMillis(90000L * 60 * 24);
    assertEquals(msg ,"1" + numberSeparator() + "50 days");
  }
}
