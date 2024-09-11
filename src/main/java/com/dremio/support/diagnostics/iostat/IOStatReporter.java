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

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;

import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IOStatReporter {
  private final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();

  public void write(final ReportStats reportStats, final OutputStream streamWriter)
      throws UnsupportedEncodingException, IOException {
    final Summary summary = this.summaryStats(reportStats);
    try (BufferedOutputStream output = new BufferedOutputStream(streamWriter)) {
      final String template =
          String.format(
              Locale.US,
              """
 <!DOCTYPE html>
 <html lang="en">
 <head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>IO Stats report</title>
  <meta name"description" content="report for iostats">
  <meta name="author" content="dremio">
  <meta property="og:title" content="iostats report">
  <meta property="og:type" content="website">
  <meta property="og:description" content="plotly generated graphs for iostats">
  <style>
     html {
      scroll-behavior: smooth;
    }
     table {
     table-layout:fixed; width: 100%%;
     }
     .summary-page {
         display: grid;
         grid-template-columns: repeat(3, 1fr);
         grid-gap: 10px;
         grid-auto-rows: minmax(100px, auto);
     }
     .content-page {
         display: grid;
         grid-template-columns: repeat(2, 1fr);
         grid-gap: 10px;
         grid-auto-rows: minmax(100px, auto);
     }
     .tooltip-pr {
       overflow: hidden;
       white-space: nowrap;
       text-overflow: ellipsis;
     }

     .tooltip-pr .tooltiptext-pr {
       color: black;
       hyphens: auto;
     }

     .tooltip-pr:hover {
       cursor: pointer;
       white-space: initial;
       transition: height 0.2s ease-in-out;
     }

     /* Style the navbar */
     #navbar {
       overflow: hidden;
       background-color: #333;
       z-index: 289;
     }

     /* Navbar links */
     #navbar a {
       float: left;
       display: block;
       color: #f2f2f2;
       text-align: center;
       padding: 14px;
       text-decoration: none;
     }
       #navbar .active-link {
         color: white;
         background-color: green;
       }

     /* Page content */
     .content {
       //padding-top: 50px;
     }

     /* The sticky class is added to the navbar with JS when it reaches its scroll position */
     .sticky {
       position: fixed;
       top: 0;
       width: 100%%;
     }

     /* Add some top padding to the page content to prevent sudden quick movement (as the navigation bar gets a new position at the top of the page (position:fixed and top:0) */
     .sticky + .content {
       padding-top: 100px;
     }
 </style>
  <style>
    %s
  </style>
  <script>
  %s
  </script>
   <script>
  %s
  </script>
  <style>
    %s
  </style>
  <script>
  %s
  </script>
     <script>
  %s
  </script>
 </head>
 <body>
 <div id="navbar">
   <div style="float: left;">
   <h3 style="color: white" >IO Stat Analysis</h3>
   </div>
   <div style="float:right;">
   <a class="nav-link" href="#summary-section">Summary</a>
   <a class="nav-link" href="#cpu-section">CPU</a>
   <a class="nav-link" href="#disk-queue-section">Queue</a>
   <a class="nav-link" href="#disk-await-section">Await</a>
   <a class="nav-link" href="#disk-rw-section">Throughput</a>
   <a class="nav-link" href="#disk-iops-section">IOPS</a>
   <a class="nav-link" href="#disk-util-section">Utilization</a>
   </div>
 </div>
 <main class="content">
 <section id="summary-section">
 <h3>Summary</h3>
 <div class="content-page">
  <div>%s</div>
  <div>%s</div>
 </div>
 </section>
 %s
 </main>
 <script>
   // When the user scrolls the page, execute myFunction
   window.onscroll = function() {stickNav()};

   // Get the navbar
   var navbar = document.getElementById("navbar");

   // Get the offset position of the navbar
   var sticky = navbar.offsetTop;

   // Add the sticky class to the navbar when you reach its scroll position. Remove "sticky" when you leave the scroll position
   function stickNav() {
     if (window.pageYOffset >= sticky) {
       navbar.classList.add("sticky")
     } else {
       navbar.classList.remove("sticky");
     }
   }
 </script>
  <script>
    const sections = document.querySelectorAll('section');
    const links = document.querySelectorAll('a.nav-link');

    window.addEventListener('scroll', () => {
        let scrollPosition = window.scrollY + 80;
        sections.forEach(section => {
            if (scrollPosition >= section.offsetTop) {
                links.forEach(link => {
                    link.classList.remove('active-link');
                    if (section.getAttribute('id') === link.getAttribute('href').substring(1)) {
                        link.classList.add('active-link');
                    }
                });
            }
        });
    });
  </script>
 </body>
</html>
""",
              jsLibraryTextProvider.getTableCSS(),
              jsLibraryTextProvider.getPlotlyJsText(),
              jsLibraryTextProvider.getCSVExportText(),
              jsLibraryTextProvider.getSortableCSSText(),
              jsLibraryTextProvider.getSortableText(),
              jsLibraryTextProvider.getFilterTableText(),
              this.summaryText(summary),
              this.recommendations(summary),
              this.detailGraph(reportStats));
      output.write(template.getBytes("UTF-8"));
    }
  }

  Summary summaryStats(final ReportStats reportStats) {
    final long totalRecords = reportStats.cpuStats().size();
    final double percentageTimeOver50;
    if (reportStats.numberOfTimesOver50PerCpu() > 0) {
      percentageTimeOver50 =
          ((double) reportStats.numberOfTimesOver50PerCpu() / (double) totalRecords) * 100.0;
    } else {
      percentageTimeOver50 = 0.0;
    }
    final double percentageTimeOver90;
    if (reportStats.numberOfTimesOver90PerCpu() > 0) {
      percentageTimeOver90 =
          ((double) reportStats.numberOfTimesOver90PerCpu() / (double) totalRecords) * 100.0;
    } else {
      percentageTimeOver90 = 0.0;
    }
    final double percentageIOWaitTimeOver5;
    if (reportStats.ioBottleneckCount() > 0) {
      percentageIOWaitTimeOver5 =
          ((double) reportStats.ioBottleneckCount() / (double) totalRecords) * 100.0;
    } else {
      percentageIOWaitTimeOver5 = 0.0;
    }
    List<String> disks = new ArrayList<>();
    for (String key : reportStats.queueMap().keySet()) {
      disks.add(key);
    }
    Collections.sort(disks);
    final Map<String, Double> diskPer = new HashMap<>();
    for (final String deviceName : disks) {
      Long timesQueued = reportStats.queueMap().get(deviceName);
      final double percentageQueued;
      if (reportStats.ioBottleneckCount() > 0) {
        percentageQueued = ((double) timesQueued / (double) totalRecords) * 100.0;
      } else {
        percentageQueued = 0.0;
      }
      diskPer.put(deviceName, percentageQueued);
    }
    return new Summary(
        percentageTimeOver50,
        percentageTimeOver90,
        percentageIOWaitTimeOver5,
        disks,
        diskPer,
        totalRecords);
  }

  String summaryText(final Summary summary) {
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    final Collection<Collection<HtmlTableDataColumn<String, Long>>> rows = new ArrayList<>();
    rows.add(
        Arrays.asList(
            col("number measurements"),
            col(String.format(Locale.US, "%,d", summary.totalRecords()))));
    rows.add(
        Arrays.asList(
            col(
                "%time over 50% user+system+steal+nice cpu usage (for systems with 2 threads per"
                    + " core)"),
            col(String.format(Locale.US, "%.2f%%", summary.percOver50()))));
    rows.add(
        Arrays.asList(
            col(
                "%time over 90% user+system+steal+nice cpu usage (for systems with 1 thread per"
                    + " core)"),
            col(String.format(Locale.US, "%.2f%%", summary.percOver90()))));
    rows.add(
        Arrays.asList(
            col("%time over iowait% is over 5%"),
            col(String.format(Locale.US, "%.2f%%", summary.percIOWaitOver5()))));
    for (final String deviceName : summary.diskNames()) {
      Double percentageQueued = summary.queuePerc().get(deviceName);
      rows.add(
          Arrays.asList(
              col(String.format("%%time %s had queue depth over 1.0", deviceName)),
              col(String.format(Locale.US, "%.2f%%", percentageQueued))));
    }
    return builder.generateTable(
        "summaryStatsTable", "Recommendations", Arrays.asList("name", "value"), rows);
  }

  String recommendations(final Summary summary) {
    final HtmlTableBuilder builder = new HtmlTableBuilder();
    final Collection<Collection<HtmlTableDataColumn<String, String>>> rows = new ArrayList<>();
    int counter = 0;
    if (summary.percIOWaitOver5() > 10.0) {
      List<String> problemDisks = new ArrayList<>();
      for (String disk : summary.diskNames()) {
        Double per = summary.queuePerc().get(disk);
        if (per > 10.0) {
          problemDisks.add(disk);
        }
      }
      if (problemDisks.size() > 0) {
        counter++;
        var rec =
            col(
                "Increase iops and throughput capacity on the following disks: %s",
                String.join(",", problemDisks));
        rows.add(Arrays.asList(col(String.valueOf(counter)), rec));
      } else {
        counter++;
        var rec =
            col(
                "The cpu is often waiting on the IO layer, this could be network. None of the disks"
                    + " are substantially saturated.",
                String.join(",", problemDisks));
        rows.add(Arrays.asList(col(String.valueOf(counter)), rec));
      }
    }
    if (summary.percOver50() > 10.0) {
      counter++;
      final HtmlTableDataColumn<String, String> rec =
          col(
              "For systems with 2 threads per core (look for lscpu output in the os_info.txt file"
                  + " of DDC to determine this), the CPU utilization is too high. Increase CPU"
                  + " count or reduce workload.");
      rows.add(Arrays.asList(col(String.valueOf(counter)), rec));
    }
    if (summary.percOver90() > 10.0) {
      counter++;
      final HtmlTableDataColumn<String, String> rec =
          col(
              "For systems with 1 thread per core (look for lscpu output in the os_info.txt file of"
                  + " DDC to determine this), the CPU utilization is too high. Increase CPU count"
                  + " or reduce workload.");
      rows.add(Arrays.asList(col(String.valueOf(counter)), rec));
    }
    return builder.generateTable(
        "summaryStatsTable", "Important Data Points", Arrays.asList("#", "recommendation"), rows);
  }

  String detailGraph(final ReportStats reportStats) {
    List<Double> userList = new ArrayList<>();
    List<Double> idleList = new ArrayList<>();
    List<Double> stealList = new ArrayList<>();
    List<Double> sysList = new ArrayList<>();
    List<Double> iowaitList = new ArrayList<>();
    List<Double> niceList = new ArrayList<>();

    for (final CPUStats cpu : reportStats.cpuStats()) {
      userList.add((double) cpu.user());
      idleList.add((double) cpu.idle());
      stealList.add((double) cpu.steal());
      sysList.add((double) cpu.system());
      iowaitList.add((double) cpu.iowait());
      niceList.add((double) cpu.nice());
    }
    List<LocalDateTime> times = reportStats.times();
    List<String> cpuTraces = new ArrayList<>();
    cpuTraces.add(makeTrace(times, userList, "user%"));
    cpuTraces.add(makeTrace(times, sysList, "sys%"));
    cpuTraces.add(makeTrace(times, iowaitList, "iowait%"));
    cpuTraces.add(makeTrace(times, niceList, "nice%"));
    cpuTraces.add(makeTrace(times, stealList, "steal%"));
    cpuTraces.add(makeTrace(times, idleList, "idle%"));

    List<String> diskQueueTraces = new ArrayList<>();
    List<String> disks = new ArrayList<>();
    for (final String d : reportStats.diskMap().keySet()) {
      disks.add(d);
    }
    Collections.sort(disks);
    for (final String d : disks) {
      List<Double> data =
          reportStats.diskMap().get(d).stream().map(x -> x.averageQueueSize()).toList();
      diskQueueTraces.add(makeTrace(times, data, d));
    }

    List<String> diskAwaitTraces = new ArrayList<>();

    for (final String d : disks) {
      List<Double> writeData =
          reportStats.diskMap().get(d).stream().map(x -> x.writeAverageWaitMillis()).toList();
      diskAwaitTraces.add(makeTrace(times, writeData, d + " write"));
      List<Double> readData =
          reportStats.diskMap().get(d).stream().map(x -> x.readAverageWaitMillis()).toList();
      diskAwaitTraces.add(makeTrace(times, readData, d + " read"));
    }

    List<String> diskRWTraces = new ArrayList<>();

    for (final String d : disks) {
      List<Double> writeData =
          reportStats.diskMap().get(d).stream().map(x -> x.writesKBPerSecond()).toList();
      diskRWTraces.add(makeTrace(times, writeData, d + " write"));
      List<Double> readData =
          reportStats.diskMap().get(d).stream().map(x -> x.readsKBPerSecond()).toList();
      diskRWTraces.add(makeTrace(times, readData, d + " read"));
    }

    List<String> diskIOPSTraces = new ArrayList<>();

    for (final String d : disks) {
      List<Double> writeData =
          reportStats.diskMap().get(d).stream().map(x -> x.writesPerSecond()).toList();
      diskIOPSTraces.add(makeTrace(times, writeData, d + " write"));
      List<Double> readData =
          reportStats.diskMap().get(d).stream().map(x -> x.readsPerSecond()).toList();
      diskIOPSTraces.add(makeTrace(times, readData, d + " read"));
    }

    List<String> diskUtilTraces = new ArrayList<>();

    for (final String d : disks) {
      List<Double> data =
          reportStats.diskMap().get(d).stream().map(x -> x.utilizationPercentage()).toList();
      diskUtilTraces.add(makeTrace(times, data, d));
    }

    return String.format(
        Locale.US,
        """
        <section>
            <section id="cpu-section">
                <h3>CPU</h3>
                <div id="cpu-usage-graph"></div>
            </section>
            <section id="disk-queue-section">
                <h3>Queue</h3>
                <div id="disk-queue-graph"></div>
            </section>
            <section id="disk-await-section">
                <h3>Await</h3>
                <div id="disk-await-graph"></div>
            </section>
            <section id="disk-rw-section">
                <h3>Throughput</h3>
                <div id="disk-io-graph"></div>
            </section>
            <section id="disk-iops-section">
                <h3>IOPS</h3>
                <div id="disk-iops-graph"></div>
            </section>
            <section id="disk-util-section">
                <h3>Utilization</h3>
                <div id="disk-util-graph"></div>
            </section>
            <script>
            Plotly.newPlot('cpu-usage-graph',[ %s ], {
            title:'CPU Usage Over Time'
            });
            Plotly.newPlot('disk-queue-graph',[ %s ], {
            title:'Disk Queue Over Time'
            });
            Plotly.newPlot('disk-await-graph',[ %s ], {
            title:'Disk Await Millis Over Time'
            });
            Plotly.newPlot('disk-io-graph',[ %s ], {
            title:'Disk Read/Write KB Over Time'
            });
             Plotly.newPlot('disk-iops-graph',[ %s ], {
            title:'Disk IOPS Over Time'
            });
             Plotly.newPlot('disk-util-graph',[ %s ], {
            title:'Disk Util%% Over Time'
            });
        </script>
        """,
        String.join(",", cpuTraces),
        String.join(",", diskQueueTraces),
        String.join(",", diskAwaitTraces),
        String.join(",", diskRWTraces),
        String.join(",", diskIOPSTraces),
        String.join(",", diskUtilTraces));
  }

  String makeTrace(List<LocalDateTime> times, List<Double> data, String title) {
    return String.format(
        Locale.US,
        """
    {
      x: [%s],
      y: [%s],
      mode: 'lines',
      name: '%s'
}
""",
        String.join(
            ",",
            times.stream().map(x -> String.format(Locale.US, "\"%s\"", x.toString())).toList()),
        String.join(",", data.stream().map(x -> x.toString()).toList()),
        title);
  }
}
