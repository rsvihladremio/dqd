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

import com.dremio.support.diagnostics.repro.PDSDataProvider;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;

/** ConstantDataProvider is a copy from the existing reproduction tool way of generating data */
public class PDSConstantDataProvider implements PDSDataProvider {

  @Override
  public long getLong() {
    return 1;
  }

  @Override
  public float getFloat() {
    return 1.0f;
  }

  @Override
  public double getDouble() {
    return 1.0;
  }

  @Override
  public int getInt() {
    return 1;
  }

  @Override
  public String getString() {
    return "Hello world!";
  }

  @Override
  public LocalDate getLocalDate() {
    return LocalDate.of(2018, Month.SEPTEMBER, 14);
  }

  @Override
  public Instant getInstant() {
    return Instant.ofEpochSecond(1_663_200_000L);
  }

  @Override
  public LocalTime getTime() {
    return LocalTime.of(0, 0, 0);
  }

  @Override
  public boolean getBoolean() {
    return true;
  }

  @Override
  public List<String> getList() {
    final List<String> ret = new ArrayList<>();
    ret.add(getString());
    return ret;
  }

  @Override
  public String getInterval() {
    return "INTERVAL '1' DAY";
  }
}
