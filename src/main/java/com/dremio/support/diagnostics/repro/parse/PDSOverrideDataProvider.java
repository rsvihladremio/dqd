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
import com.google.common.base.Splitter;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PDSOverrideDataProvider implements PDSDataProvider {

  private final Random random = new Random();
  private final String[] possibleValues;

  public PDSOverrideDataProvider(final String... possibleValues) {
    this.possibleValues = possibleValues.clone();
  }

  private String rawValue() {
    return this.possibleValues[random.nextInt(this.possibleValues.length)];
  }

  @Override
  public long getLong() {
    return Long.valueOf(rawValue());
  }

  @Override
  public float getFloat() {
    return Float.valueOf(rawValue());
  }

  @Override
  public double getDouble() {
    return Double.valueOf(rawValue());
  }

  @Override
  public int getInt() {
    return Integer.valueOf(rawValue());
  }

  @Override
  public String getString() {
    return rawValue();
  }

  @Override
  public LocalDate getLocalDate() {
    return LocalDate.parse(rawValue());
  }

  @Override
  public Instant getInstant() {
    return Instant.parse(rawValue());
  }

  @Override
  public LocalTime getTime() {
    return LocalTime.parse(rawValue());
  }

  @Override
  public boolean getBoolean() {
    return Boolean.parseBoolean(rawValue());
  }

  @Override
  public List<String> getList() {
    final String raw = rawValue();
    final Iterable<String> items = Splitter.on(',').split(raw);
    final List<String> ret = new ArrayList<>();
    for (final String element : items) {
      ret.add(element.trim());
    }
    return ret;
  }

  @Override
  public String getInterval() {
    return String.format("INTERVAL '%s' DAY", getInt());
  }
}
