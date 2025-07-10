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
package com.dremio.support.diagnostics.top;

import com.dremio.support.diagnostics.iostat.CPUStats;
import com.dremio.support.diagnostics.shared.DQDVersion;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.text.StringEscapeUtils;

public class TopExec {
  /**
   * A composite key class to uniquely identify threads by combining PID and command.
   * This helps address the issue of duplicate or recycled PIDs.
   */
  private static class ThreadKey {
    private final String pid;
    private final String command;

    public ThreadKey(String pid, String command) {
      this.pid = pid;
      this.command = command;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ThreadKey threadKey = (ThreadKey) o;
      return pid.equals(threadKey.pid) && command.equals(threadKey.command);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pid, command);
    }

    @Override
    public String toString() {
      return pid + ":" + command;
    }
  }

  public static void exec(final InputStream file, final OutputStream writer) throws IOException {
    try (InputStreamReader inputStreamReader = new InputStreamReader(file)) {
      try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(writer)) {
          final List<LocalTime> times = new ArrayList<>();
          final Map<ThreadKey, List<ThreadUsage>> maps = new HashMap<>();
          final List<CPUStats> cpuStats = new ArrayList<>();
          final List<MemStats> memStats = new ArrayList<>();
          final List<SwapStats> swapStats = new ArrayList<>();
          final List<ThreadStats> threadStats = new ArrayList<>();
          final List<ParseError> parseErrors = new ArrayList<>();

          boolean startParsingThreads = false;
          String line = null;
          while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("Threads")) {
              try {
                // Threads: 525 total,   1 running, 524 sleeping,   0 stopped,   0 zombie
                // Extract the values using regex
                final String[] parts = line.split(",\\s*");
                final int total =
                    Integer.parseInt(parts[0].split(":")[1].trim().split(" ")[0]); // Total threads
                final int running = Integer.parseInt(parts[1].split(" ")[0]); // Running threads
                final int sleeping = Integer.parseInt(parts[2].split(" ")[0]); // Sleeping threads
                final int stopped = Integer.parseInt(parts[3].split(" ")[0]); // Stopped threads
                final int zombie = Integer.parseInt(parts[4].split(" ")[0]); // Zombie threads

                // Create an instance of ThreadStats
                final ThreadStats stats =
                    new ThreadStats(total, running, sleeping, stopped, zombie);
                threadStats.add(stats);
              } catch (final Exception ex) {
                parseErrors.add(new ParseError(ex.getMessage(), "Thread Stats"));
              }
            }
            if (line.startsWith("MiB Mem ")) {
              try {
                final String[] parts = line.split(",\\s*");
                final String total = parts[0].split(":")[1].trim().split(" ")[0]; // Total memory
                final String free = parts[1].split(" ")[0]; // Free memory
                final String used = parts[2].split(" ")[0]; // Used memory
                final String buffCache = parts[3].split(" ")[0]; // Buff/cache memory
                memStats.add(
                    new MemStats(
                        Float.parseFloat(total),
                        Float.parseFloat(free),
                        Float.parseFloat(used),
                        Float.parseFloat(buffCache)));
              } catch (final Exception ex) {
                parseErrors.add(new ParseError(ex.getMessage(), "Memory"));
              }
              continue;
            }
            if (line.startsWith("MiB Swap:")) {
              try {
                final String[] parts = line.split(",\\s*");

                final String totalPart = parts[0].trim();
                final String freePart = parts[1].trim();
                final String usedAvailPart = parts[2].trim();

                // Extract the numbers
                final String total =
                    totalPart
                        .split(" ")[
                        totalPart.split(" ").length - 2]; // Get the value before "total"
                final String free = freePart.split(" ")[0]; // Get the value before "free"
                final String[] tokens = usedAvailPart.split(" ");

                final String used = tokens[0]; // Get the value before "used."
                String availMem = "0.0";
                for (int i = 0; i < tokens.length; i++) {
                  String t = tokens[i];
                  if (t.trim().equals("avail")) {
                    availMem = tokens[i - 1];
                  }
                }

                swapStats.add(
                    new SwapStats(
                        Float.parseFloat(total),
                        Float.parseFloat(free),
                        Float.parseFloat(used),
                        Float.parseFloat(availMem)));
              } catch (final Exception ex) {
                parseErrors.add(new ParseError(ex.getMessage(), "Swap"));
              }
              continue;
            }
            if (line.startsWith("top - ")) {
              // top - 12:02:04 up  3:07,  0 users,  load average: 3.18, 1.16, 0.41
              final String[] tokens = line.trim().split("\\s+");
              final LocalTime timeStamp = LocalTime.parse(tokens[2]);
              times.add(timeStamp);
              continue;
            }
            if (line.startsWith("%Cpu(s):")) {
              // %Cpu(s): 75.3 us,  3.2 sy,  0.0 ni, 20.4 id,  0.0 wa,  0.0 hi,  1.0 si,  0.0 st
              final String[] tokens = line.trim().split("\\s+");
              final float user = Float.parseFloat(tokens[1]);
              final float sys = Float.parseFloat(tokens[3]);
              final float nice = Float.parseFloat(tokens[5]);
              final float idle = Float.parseFloat(tokens[7]);
              final float iowait = Float.parseFloat(tokens[9]);
              final float steal = Float.parseFloat(tokens[15]);
              cpuStats.add(new CPUStats(user, nice, sys, iowait, steal, idle));
              continue;
            }
            if (line.contains("PID USER")) {
              startParsingThreads = true;
              continue;
            }
            if (startParsingThreads) {
              if (line.length() == 0) {
                startParsingThreads = false;
                continue;
              }
              //    996 dremio    20   0 7008232   3.4g  98412 S  82.2  21.9   1:36.72 C2
              // CompilerThre
              final String[] tokens = line.trim().split("\\s+");

              final String pid = tokens[0];
              final Double cpu = Double.parseDouble(tokens[8]);
              final StringBuilder command = new StringBuilder();

              // Combine the remaining parts for the command
              for (int i = 11; i < tokens.length; i++) {
                command.append(tokens[i]).append(" ");
              }

              final String commandStr = command.toString().trim();
              final ThreadUsage threadUsage = new ThreadUsage(pid, cpu, commandStr);
              final ThreadKey threadKey = new ThreadKey(pid, commandStr);

              if (maps.containsKey(threadKey)) {
                final List<ThreadUsage> usage = maps.get(threadKey);
                usage.add(threadUsage);
                maps.put(threadKey, usage);
              } else {
                final List<ThreadUsage> usage = new ArrayList<>();
                usage.add(threadUsage);
                maps.put(threadKey, usage);
              }
            }
          }
          final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();
          // now generate the report
          final String html =
              String.format(
                  Locale.US,
                  """
           <!DOCTYPE html>
 <html lang="en">
 <head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Threaded Top report</title>
  <meta name"description" content="report for top">
  <meta name="author" content="dremio">
  <meta property="og:title" content="top report">
  <meta property="og:type" content="website">
  <meta property="og:description" content="plotly generated graphs for top">
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
 </head>
 <body>
 <div id="navbar">
   <div style="float: left;">
   <h3 style="color: white" >Thread Top Analysis</h3>
   </div>
   <div style="float:right;">
   <a class="nav-link" href="#cpu-section">CPU</a>
   <a class="nav-link" href="#threads-section">Threads</a>
   <a class="nav-link" href="#mem-section">Memory</a>
   <a class="nav-link" href="#swap-section">Swap</a>
   <a class="nav-link" href="#thread-stats-section">Thread Stats</a>
   <a class="nav-link" href="#debugging-section">Report Debugging</a>
   </div>
 </div>
 <main class="content">
 <p>To understand this report consider reading <a href="https://www.redhat.com/sysadmin/interpret-top-output">this description of how to read top</a></p>
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
                  threadGraph(
                      times, cpuStats, memStats, swapStats, threadStats, parseErrors, maps));
          writer.write(html.getBytes("UTF-8"));
        }
      }
    }
  }

  /**
   * @param times
   * @param cpuStats
   * @param memStats
   * @param swapStats
   * @param threadStats
   * @param parseErrors
   * @param map
   * @return
   */
  private static String threadGraph(
      final List<LocalTime> times,
      final List<CPUStats> cpuStats,
      final List<MemStats> memStats,
      final List<SwapStats> swapStats,
      final List<ThreadStats> threadStats,
      final List<ParseError> parseErrors,
      final Map<ThreadKey, List<ThreadUsage>> map) {

    final List<ThreadKey> keys =
        map.entrySet().stream()
            .sorted(
                (x, y) -> {
                  final Double sumx =
                      x.getValue().stream().map(c -> c.cpuUsage()).reduce(0.0, (a, b) -> a + b);
                  final Double sumy =
                      y.getValue().stream().map(c -> c.cpuUsage()).reduce(0.0, (a, b) -> a + b);
                  return sumy.compareTo(sumx);
                })
            .map(x -> x.getKey())
            .toList();
    List<String> threadTraces = new ArrayList<>();
    for (ThreadKey key : keys) {
      List<ThreadUsage> usage = map.get(key);
      if (usage.size() == 0) {
        continue;
      }
      String commandName = key.command;
      List<Float> data = usage.stream().map(x -> (float) x.cpuUsage()).toList();
      threadTraces.add(makeTrace(times, data, commandName));
    }

    List<Float> userList = new ArrayList<>();
    List<Float> idleList = new ArrayList<>();
    List<Float> stealList = new ArrayList<>();
    List<Float> sysList = new ArrayList<>();
    List<Float> iowaitList = new ArrayList<>();
    List<Float> niceList = new ArrayList<>();

    for (final CPUStats cpu : cpuStats) {
      userList.add(cpu.user());
      idleList.add(cpu.idle());
      stealList.add(cpu.steal());
      sysList.add(cpu.system());
      iowaitList.add(cpu.iowait());
      niceList.add(cpu.nice());
    }

    List<String> cpuTraces = new ArrayList<>();
    cpuTraces.add(makeTrace(times, userList, "user"));
    cpuTraces.add(makeTrace(times, sysList, "sys"));
    cpuTraces.add(makeTrace(times, iowaitList, "iowait"));
    cpuTraces.add(makeTrace(times, niceList, "nice"));
    cpuTraces.add(makeTrace(times, stealList, "steal"));
    cpuTraces.add(makeTrace(times, idleList, "idle"));

    List<Float> totalMemList = new ArrayList<>();
    List<Float> freeMemList = new ArrayList<>();
    List<Float> usedMemList = new ArrayList<>();
    List<Float> bufferMemList = new ArrayList<>();

    for (final MemStats mem : memStats) {
      totalMemList.add(mem.total());
      freeMemList.add(mem.free());
      usedMemList.add(mem.used());
      bufferMemList.add(mem.buffCache());
    }

    final List<String> memoryTraces = new ArrayList<>();
    memoryTraces.add(makeTrace(times, totalMemList, "total"));
    memoryTraces.add(makeTrace(times, freeMemList, "free"));
    memoryTraces.add(makeTrace(times, usedMemList, "used"));
    memoryTraces.add(makeTrace(times, bufferMemList, "buffer/page"));

    List<Float> totalSwapList = new ArrayList<>();
    List<Float> freeSwapList = new ArrayList<>();
    List<Float> usedSwapList = new ArrayList<>();
    List<Float> availSwapList = new ArrayList<>();

    for (final SwapStats swap : swapStats) {
      totalSwapList.add(swap.total());
      freeSwapList.add(swap.free());
      usedSwapList.add(swap.used());
      availSwapList.add(swap.avail());
    }

    final List<String> swapTraces = new ArrayList<>();
    swapTraces.add(makeTrace(times, totalSwapList, "total"));
    swapTraces.add(makeTrace(times, freeSwapList, "free"));
    swapTraces.add(makeTrace(times, usedSwapList, "used"));
    swapTraces.add(makeTrace(times, availSwapList, "avail"));

    List<Integer> totalThreadsList = new ArrayList<>();
    List<Integer> runningThreadsList = new ArrayList<>();
    List<Integer> sleepingThreadsList = new ArrayList<>();
    List<Integer> stoppedThreadsList = new ArrayList<>();
    List<Integer> zombieThreadsList = new ArrayList<>();

    for (final ThreadStats t : threadStats) {
      totalThreadsList.add(t.total());
      runningThreadsList.add(t.running());
      sleepingThreadsList.add(t.sleeping());
      stoppedThreadsList.add(t.stopped());
      zombieThreadsList.add(t.zombie());
    }

    final List<String> threadStatsTraces = new ArrayList<>();
    threadStatsTraces.add(makeTrace(times, totalThreadsList, "total"));
    threadStatsTraces.add(makeTrace(times, runningThreadsList, "running"));
    threadStatsTraces.add(makeTrace(times, sleepingThreadsList, "sleeping"));
    threadStatsTraces.add(makeTrace(times, stoppedThreadsList, "stopped"));
    threadStatsTraces.add(makeTrace(times, zombieThreadsList, "zombie"));

    final List<Collection<HtmlTableDataColumn<String, Integer>>> rows = new ArrayList<>();
    for (int i = 0; i < parseErrors.size(); i++) {
      final ParseError parseError = parseErrors.get(i);
      final String e = parseError.msg();
      final String c = parseError.category();
      HtmlTableDataColumn<String, Integer> numCol = HtmlTableDataColumn.col(String.valueOf(i), i);
      HtmlTableDataColumn<String, Integer> errCol = HtmlTableDataColumn.col(e);
      HtmlTableDataColumn<String, Integer> catCol = HtmlTableDataColumn.col(c);
      List<HtmlTableDataColumn<String, Integer>> row = Arrays.asList(numCol, errCol, catCol);
      rows.add(row);
    }
    final List<Collection<HtmlTableDataColumn<String, Integer>>> reportRows = new ArrayList<>();
    List<HtmlTableDataColumn<String, Integer>> row =
        Arrays.asList(
            HtmlTableDataColumn.col("report version"),
            HtmlTableDataColumn.col(DQDVersion.getVersion()));
    reportRows.add(row);
    return String.format(
        Locale.US,
        """
        <section id="cpu-section">
         <h3>CPU</h3>
         <div id="top-cpu-graph"></div>
         </section>
         <section id="threads-section">
         <h3>Threads</h3>
          <div id="threads-usage-graph"></div>
        </section>
         <section id="mem-section">
         <h3>Memory</h3>
         <div id="top-mem-graph"></div>
         </section>
          <section id="swap-section">
         <h3>Swap</h3>
         <div id="top-swap-graph"></div>
         </section>
        <section id="thread-stats-section">
         <h3>Thread Stats</h3>
         <div id="top-threads-graph"></div>
         </section>
          <section id="debugging-section">
         <h3>Debugging</h3>
         <h4>Report Stats</h4>
         %s
         <h4>Parse Errors</h4>
         %s
         </section>
        <script>
        Plotly.newPlot('top-cpu-graph', [ %s ], {
          title:'CPU Usage'
        });
        Plotly.newPlot('threads-usage-graph',[ %s ], {
          title:'Per Thread CPU Usage (top 100 threads)'
        });
        Plotly.newPlot('top-mem-graph',[ %s ], {
          title:'Mem Usage in kb (base 1000)'
        });
         Plotly.newPlot('top-swap-graph',[ %s ], {
          title:'Swap Usage in kb (base 1000)'
        });
          Plotly.newPlot('top-threads-graph',[ %s ], {
          title:'Thread Stats'
        });
        </script>

        """,
        new HtmlTableBuilder()
            .generateTable(
                "reportStats", "report statistics", Arrays.asList("name", "value"), reportRows),
        new HtmlTableBuilder()
            .generateTable(
                "parsingErrors",
                "errors during parsing",
                Arrays.asList("#", "error", "category"),
                rows),
        String.join(",", cpuTraces),
        String.join(",", threadTraces),
        String.join(",", memoryTraces),
        String.join(",", swapTraces),
        String.join(",", threadStatsTraces));
  }

  static <T> String makeTrace(final List<LocalTime> times, final List<T> data, final String title) {
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
        String.join(",", times.stream().map(x -> String.format(Locale.US, "\"%s\"", x)).toList()),
        String.join(",", data.stream().map(x -> x.toString()).toList()),
        StringEscapeUtils.escapeEcmaScript(title));
  }
}
