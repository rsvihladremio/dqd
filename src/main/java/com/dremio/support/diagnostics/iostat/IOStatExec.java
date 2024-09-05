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
package com.dremio.support.diagnostics.iostat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IOStatExec {

  private final IOStatReporter reporter = new IOStatReporter();

  public static void exec(final InputStream is, final OutputStream writer) throws IOException {
    new IOStatExec().run(is, writer);
  }

  void run(final InputStream is, final OutputStream writer) throws IOException {
    final ReportStats reportModel = parseReport(is);
    reporter.write(reportModel, writer);
  }

  ReportStats parseReport(final InputStream is) throws IOException {
    final List<CPUStats> cpuStats = new ArrayList<>();
    final Map<String, List<DiskStats>> diskMap = new HashMap<>();
    final List<LocalDateTime> times = new ArrayList<>();
    Map<Integer, String> diskMapLocations = null;

    Pattern pattern1 = Pattern.compile("\\d{2}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}");
    Pattern pattern2 = Pattern.compile("\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2} [APM]{2}");
    try (final InputStreamReader isReader = new InputStreamReader(is)) {
      try (final BufferedReader reader = new BufferedReader(isReader)) {
        DateTimeFormatter formatter =
            new DateTimeFormatterBuilder()
                .appendOptional(DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss"))
                .appendOptional(DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a"))
                .toFormatter(Locale.ENGLISH);
        String line = null;
        boolean readCpuLine = false;
        boolean readDevices = false;
        while ((line = reader.readLine()) != null) {
          final Matcher matcher1 = pattern1.matcher(line);
          if (matcher1.find()) {
            String dateString = matcher1.group();
            LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
            times.add(dateTime);
            continue;
          }
          final Matcher matcher2 = pattern2.matcher(line);
          if (matcher2.find()) {
            String dateString = matcher2.group();
            LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
            times.add(dateTime);
            continue;
          }
          if (startCpuParseNextRow(line)) {
            readCpuLine = true;
            continue;
          }
          if (readCpuLine) {
            cpuStats.add(parseCpuStats(line));
            readCpuLine = false;
            continue;
          }
          if (startReadingDevicesNextLine(line)) {
            readDevices = true;
            if (diskMapLocations == null) {
              diskMapLocations = parseLocationOfDiskStatTokens(line);
            }
            continue;
          }
          if (readDevices) {
            if (line.length() == 0) {
              readDevices = false;
              continue;
            }

            final DiskStats diskStats = parseDiskStats(line, diskMapLocations);
            final String name = diskStats.name();
            if (diskMap.containsKey(name)) {
              List<DiskStats> list = diskMap.get(name);
              list.add(diskStats);
              diskMap.put(name, list);
            } else {
              List<DiskStats> list = new ArrayList<>();
              list.add(diskStats);
              diskMap.put(name, list);
            }
            continue;
          }
        }
      }
    }
    long numberOfTimesCPUOver50 = cpuStats.stream().filter(x -> x.getNonIOUsage() > 50.0f).count();
    long numberOfTimesCPUOver90 = cpuStats.stream().filter(x -> x.getNonIOUsage() > 90.0f).count();
    long numberOfTimesIOWaitOver5 = cpuStats.stream().filter(x -> x.iowait() > 5.0f).count();
    final Map<String, Long> queueMap = new HashMap<>();
    for (final Map.Entry<String, List<DiskStats>> d : diskMap.entrySet()) {
      queueMap.put(
          d.getKey(), d.getValue().stream().filter(x -> x.averageQueueSize() > 1.0).count());
    }

    return new ReportStats(
        times,
        numberOfTimesCPUOver50,
        numberOfTimesCPUOver90,
        numberOfTimesIOWaitOver5,
        queueMap,
        diskMap,
        cpuStats);
  }

  CPUStats parseCpuStats(final String line) {
    // " 33.25 0.00 7.94 0.74 0.00 58.06"
    final String[] tokens = line.split("\\s+");
    List<String> filtered = new ArrayList<>();
    for (String t : tokens) {
      if (t.equals("")) {
        continue;
      }
      filtered.add(t);
    }
    return new CPUStats(
        Float.parseFloat(filtered.get(0)),
        Float.parseFloat(filtered.get(1)),
        Float.parseFloat(filtered.get(2)),
        Float.parseFloat(filtered.get(3)),
        Float.parseFloat(filtered.get(4)),
        Float.parseFloat(filtered.get(5)));
  }

  boolean startCpuParseNextRow(final String line) {
    return line.contains("avg-cpu:  %user   %nice %system %iowait  %steal   %idle");
  }

  boolean startReadingDevicesNextLine(final String line) {
    return line.startsWith("Device");
  }

  Map<Integer, String> parseLocationOfDiskStatTokens(final String line) {
    // Device r/s rkB/s rrqm/s %rrqm r_await rareq-sz w/s wkB/s wrqm/s
    // %wrqm w_await wareq-sz d/s dkB/s drqm/s %drqm d_await dareq-sz f/s f_await
    // aqu-sz %util
    final String[] tokens = line.split(" ");
    final String[] namesWeCareAbout =
        new String[] {
          "r/s",
          "rkB/s",
          "r_await",
          "rareq-sz",
          "w/s",
          "wkB/s",
          "w_await",
          "wareq-sz",
          "aqu-sz",
          "avgqu-sz",
          "%util"
        };
    final List<String> filtered = new ArrayList<>();
    for (final String t : tokens) {
      if (t.equals("")) {
        continue;
      }
      filtered.add(t);
    }
    final Map<Integer, String> results = new HashMap<>();
    // naive implementation not very fast but small data sizes so it should be fine
    for (int i = 0; i < filtered.size(); i++) {
      final String t = filtered.get(i);
      for (int j = 0; j < namesWeCareAbout.length; j++) {
        final String name = namesWeCareAbout[j];
        if (t.equals(name)) {
          // we can get away with this as the device name is unlikely to match here
          // but we should filter it out to be more correct
          results.put(i, t);
          continue;
        }
      }
    }
    return results;
  }

  DiskStats parseDiskStats(final String line, final Map<Integer, String> orderMap) {
    final List<String> filtered = new ArrayList<>();
    final String[] tokens = line.split("\\s+");
    for (final String t : tokens) {
      if (t.equals("")) {
        continue;
      }
      filtered.add(t);
    }
    final Map<String, Double> results = new HashMap<>();
    // skip the first one, it is the device name
    for (int i = 1; i < tokens.length; i++) {
      if (!orderMap.containsKey(i)) {
        continue;
      }
      final String k = orderMap.get(i);
      final Double value = Double.parseDouble(filtered.get(i));
      results.put(k, value);
    }
    final String deviceName = tokens[0];
    final Double readsSecond = results.getOrDefault("r/s", 0.0);
    final Double readsKbSecond = results.getOrDefault("rkB/s", 0.0);
    final Double rAwait = results.getOrDefault("r_await", 0.0);
    final Double readRequestSize = results.getOrDefault("rareq-sz", 0.0);
    final Double writesSecond = results.getOrDefault("w/s", 0.0);
    final Double writesKbSecond = results.getOrDefault("wkB/s", 0.0);
    final Double wAwait = results.getOrDefault("w_await", 0.0);
    final Double writesRequestSize = results.getOrDefault("wareq-sz", 0.0);

    final Double avgQueueSize;
    if (results.containsKey("avgqu-sz")) {
      avgQueueSize = results.getOrDefault("avgqu-sz", 0.0);
    } else {
      avgQueueSize = results.getOrDefault("aqu-sz", 0.0);
    }
    final Double utilPerc = results.getOrDefault("%util", 0.0);
    return new DiskStats(
        deviceName,
        readsSecond,
        readsKbSecond,
        rAwait,
        readRequestSize,
        writesSecond,
        writesKbSecond,
        wAwait,
        writesRequestSize,
        avgQueueSize,
        utilPerc);
  }
}
