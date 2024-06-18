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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Especially useful when you want to generate a lot of data, this is not yet intelligent enough to
 * take arguments for changing the cardinality of various fields, but the ranges should generate
 * somewhat intelligent looking data.
 */
public class PDSRandomDataProvider implements PDSDataProvider {
  private final Random random;

  private final TimeProvider timeProvider;

  private final String[] list =
      new String[] {
        "Ox",
        "Crab",
        "Panda",
        "Cat",
        "Dog",
        "Orca",
        "Shark",
        "Rat",
        "Mouse",
        "Deer",
        "Elephant",
        "Tiger",
        "Starfish",
        "Red Snapper",
        "Shrimp",
        "Dourado",
        "Lion",
        "Giraffe",
        "Monkey",
        "Gorilla"
      };

  public PDSRandomDataProvider(final long seed, final TimeProvider timeProvider) {
    this.random = new Random(seed);
    this.timeProvider = timeProvider;
  }

  public PDSRandomDataProvider() {
    this.random = new Random();
    this.timeProvider = new NowTimeProvider();
  }

  @Override
  public long getLong() {
    // spotbugs caught this for MIN_VALUE we cannot rely on abc, explained here
    // https://codeql.github.com/codeql-query-help/java/java-abs-of-random/
    long value = random.nextLong();
    if (value < 0) {
      value++;
    }
    return Math.abs(value);
  }

  @Override
  public float getFloat() {
    return Math.abs(random.nextFloat());
  }

  @Override
  public double getDouble() {
    return Math.abs(random.nextDouble());
  }

  @Override
  public int getInt() {
    return Math.abs(random.nextInt(1_000_000));
  }

  @Override
  public String getString() {
    return getRandomString();
  }

  @Override
  public LocalDate getLocalDate() {
    return getRandomDateTime().toLocalDate();
  }

  @Override
  public Instant getInstant() {
    return getRandomDateTime().toInstant(ZoneOffset.UTC);
  }

  @Override
  public LocalTime getTime() {
    final LocalDateTime randomDateTime = getRandomDateTime();
    return randomDateTime.toLocalTime();
  }

  @Override
  public boolean getBoolean() {
    return random.nextBoolean();
  }

  @Override
  public List<String> getList() {
    final int items = this.random.nextInt(10);
    final List<String> ret = new ArrayList<>();
    for (int i = 0; i < items; i++) {
      ret.add(getRandomString());
    }
    return ret;
  }

  private LocalDateTime getRandomDateTime() {
    // two years in seconds
    final int twoYears = 63_072_000;
    // now in seconds
    final long nowSeconds = this.timeProvider.getInstant().toEpochMilli() / 1000;
    // minumum time
    final long minTime = nowSeconds - twoYears;
    // random with a top value of the two years then add the minTime to provide
    // any second over the last two years
    // this should provide more realistic dates
    final long epochSecond = (long) random.nextInt(twoYears) + minTime;
    return LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
  }

  private String getRandomString() {
    return getAnimal() + "-" + getAnimal() + "-" + getAnimal();
  }

  private String getAnimal() {
    final int index = this.random.nextInt(list.length - 1);
    return this.list[index];
  }

  @Override
  public String getInterval() {
    return String.format("INTERVAL '%d' DAY", getInt());
  }
}
