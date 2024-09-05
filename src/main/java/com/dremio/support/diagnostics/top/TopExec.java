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
import com.dremio.support.diagnostics.shared.JsLibraryTextProvider;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TopExec {
  public static void exec(final InputStream file, final OutputStream writer) throws IOException {
    try (InputStreamReader inputStreamReader = new InputStreamReader(file)) {
      try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(writer)) {
          final List<LocalTime> times = new ArrayList<>();
          Map<String, List<ThreadUsage>> maps = new HashMap<>();
          List<CPUStats> cpuStats = new ArrayList<>();
          boolean startParsingThreads = false;
          String line = null;
          while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("top - ")) {
              // top - 12:02:04 up  3:07,  0 users,  load average: 3.18, 1.16, 0.41
              final String[] tokens = line.trim().split("\\s+");
              LocalTime timeStamp = LocalTime.parse(tokens[2]);
              times.add(timeStamp);
              continue;
            }
            if (line.startsWith("%Cpu(s):")) {
              // %Cpu(s): 75.3 us,  3.2 sy,  0.0 ni, 20.4 id,  0.0 wa,  0.0 hi,  1.0 si,  0.0 st
              final String[] tokens = line.trim().split("\\s+");
              float user = Float.parseFloat(tokens[1]);
              float sys = Float.parseFloat(tokens[3]);
              float nice = Float.parseFloat(tokens[5]);
              float idle = Float.parseFloat(tokens[7]);
              float iowait = Float.parseFloat(tokens[9]);
              float steal = Float.parseFloat(tokens[15]);
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
              if (maps.containsKey(pid)) {
                final List<ThreadUsage> usage = maps.get(pid);
                usage.add(threadUsage);
                maps.put(pid, usage);
              } else {
                final List<ThreadUsage> usage = new ArrayList<>();
                usage.add(threadUsage);
                maps.put(pid, usage);
              }
            }
          }
          final JsLibraryTextProvider jsLibraryTextProvider = new JsLibraryTextProvider();
          // now generate the report
          String html =
              String.format(
                  Locale.US,
                  """
           <!DOCTYPE html>
 <html lang="en">
 <head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>IO Stats report</title>
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
   </div>
 </div>
 <main class="content">
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
 </body>
</html>
""",
                  jsLibraryTextProvider.getTableCSS(),
                  jsLibraryTextProvider.getPlotlyJsText(),
                  threadGraph(times, cpuStats, maps));
          writer.write(html.getBytes("UTF-8"));
        }
      }
    }
  }

  private static String threadGraph(
      final List<LocalTime> times,
      final List<CPUStats> cpuStats,
      final Map<String, List<ThreadUsage>> map) {

    List<String> names =
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
    for (String name : names) {
      List<ThreadUsage> usage = map.get(name);
      if (usage.size() == 0) {
        continue;
      }
      String commandName = usage.get(0).command();
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
        <script>
        Plotly.newPlot('top-cpu-graph', [ %s ], {
          title:'CPU Usage'
        });
        Plotly.newPlot('threads-usage-graph',[ %s ], {
          title:'Per Thread CPU Usage'
        });
        </script>
        """,
        String.join(",", cpuTraces),
        String.join(",", threadTraces));
  }

  static String makeTrace(List<LocalTime> times, List<Float> data, String title) {
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
        title);
  }
}
