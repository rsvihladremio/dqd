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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * The intent of this class is to provide embeded javascript in an html page so
 * that it is easy to
 * save just the html and have a fully interactive functioning offline report
 */
public class JsLibraryTextProvider {

  private String getLib(String libraryNameAndVersion) {
    try (InputStream plotlyJsInput =
        this.getClass()
            .getResourceAsStream(String.format("/com/dremio/support/%s", libraryNameAndVersion))) {
      return new BufferedReader(new InputStreamReader(plotlyJsInput, StandardCharsets.UTF_8))
          .lines()
          .collect(Collectors.joining("\n"));
    } catch (IOException e) {
      throw new UnableToReadJsException("plotly", e);
    }
  }

  /**
   * returns plotly graphing library https://plotly.com/javascript/
   *
   * @return embedded minified plotly javascript suitable for embedding in a
   *         script tag
   */
  public String getPlotlyJsText() {
    return getLib("plotly-2.18.0.min.js");
  }

  /**
   * returns MermaidJs diagramming library https://github.com/mermaid-js/mermaid
   * this is used to
   * generate convert to rel and final physical transformation visualizations
   *
   * @return embedded minified mermaid javascript suitable for embedding in a
   *         script tag
   */
  public String getMermaidJsText() {
    return getLib("mermaid-9.3.0.min.js");
  }

  public String getHtml2CanvasText() {
    return getLib("html2canvas.js");
  }

  /**
   * @return embedded JS minified from https://github.com/tofsjonas/sortable
   *         version 2.3.2
   */
  public String getSortableText() {
    return """
                document.addEventListener("click",function(c){try{function h(b,a){return b.nodeName===a?b:h(b.parentNode,a)}var w=c.shiftKey||c.altKey,d=h(c.target,"TH"),m=d.parentNode,n=m.parentNode,g=n.parentNode;function p(b,a){b.classList.remove("dir-d");b.classList.remove("dir-u");a&&b.classList.add(a)}function q(b){var a;return w?b.dataset.sortAlt:null!==(a=b.dataset.sort)&&void 0!==a?a:b.textContent}if("THEAD"===n.nodeName&&g.classList.contains("sortable")&&!d.classList.contains("no-sort")){var r,f=m.cells,
                t=parseInt(d.dataset.sortTbr);for(c=0;c<f.length;c++)f[c]===d?r=parseInt(d.dataset.sortCol)||c:p(f[c],"");f="dir-d";if(d.classList.contains("dir-d")||g.classList.contains("asc")&&!d.classList.contains("dir-u"))f="dir-u";p(d,f);var x="dir-u"===f,y=g.classList.contains("n-last"),u=function(b,a,e){a=q(a.cells[e]);b=q(b.cells[e]);if(y){if(""===a&&""!==b)return-1;if(""===b&&""!==a)return 1}e=Number(a)-Number(b);a=isNaN(e)?a.localeCompare(b):e;return x?-a:a};for(c=0;c<g.tBodies.length;c++){var k=g.tBodies[c],
                v=[].slice.call(k.rows,0);v.sort(function(b,a){var e=u(b,a,r);return 0!==e||isNaN(t)?e:u(b,a,t)});var l=k.cloneNode();l.append.apply(l,v);g.replaceChild(l,k)}}}catch(h){}});
                        """;
  }

  /**
   * @return embedded css minified from
   *         https://github.com/tofsjonas/sortable#note-about-cssscss version
   *         2.3.2
   *         using
   *         https://cdn.jsdelivr.net/gh/tofsjonas/sortable@latest/sortable-base.min.css
   */
  public String getSortableCSSText() {
    return """
                .sortable thead th:not(.no-sort){cursor:pointer}.sortable thead th:not(.no-sort)::after,.sortable thead th:not(.no-sort)::before{transition:color .1s ease-in-out;font-size:1.2em;color:rgba(0,0,0,0)}.sortable thead th:not(.no-sort)::after{margin-left:3px;content:"▸"}.sortable thead th:not(.no-sort):hover::after{color:inherit}.sortable thead th:not(.no-sort).dir-d::after{color:inherit;content:"▾"}.sortable thead th:not(.no-sort).dir-u::after{color:inherit;content:"▴"}.sortable thead th:not(.no-sort).indicator-left::after{content:""}.sortable thead th:not(.no-sort).indicator-left::before{margin-right:3px;content:"▸"}.sortable thead th:not(.no-sort).indicator-left:hover::before{color:inherit}.sortable thead th:not(.no-sort).indicator-left.dir-d::before{color:inherit;content:"▾"}.sortable thead th:not(.no-sort).indicator-left.dir-u::before{color:inherit;content:"▴"}/*# sourceMappingURL=sortable-base.min.css.map */
                """;
  }

  public String getTableCSS() {
    return "caption {\n"
        + "font-weight: bold;\n"
        + "font-size: 24px;\n"
        + "text-align: left;\n"
        + "color: #333;\n"
        + "\tmargin-bottom: 16px;\n"
        + "\tmargin-top: 16px;\n"
        + "}\n"
        + "table {\n"
        + "border-collapse: collapse;\n"
        + "text-align: center;\n"
        + "vertical-align: middle;\n"
        + "}\n"
        + "th, td {\n"
        + "border: 1px solid black;\n"
        + "padding: 8px;\n"
        + "}\n"
        + "thead {\n"
        + "background-color: #333;\n"
        + "color: white;\n"
        + "font-size: 0.875rem;\n"
        + "text-transform: uppercase;\n"
        + "letter-spacing: 2%%;\n"
        + "}\n"
        + "tbody tr:nth-child(odd) {\n"
        + "background-color: #fff;\n"
        + "}\n"
        + "tbody tr:nth-child(even) {\n"
        + "background-color: #eee;\n"
        + "}\n"
        + "tbody th {\n"
        + "background-color: #36c;\n"
        + "color: #fff;\n"
        + " text-align: left;\n"
        + "}\n"
        + "tbody tr:nth-child(even) th {\n"
        + "background-color: #25c;\n"
        + "}\n";
  }

  public String getCSVExportText() {
    return """
                    // Quick and simple export target #table_id into a csv
                    function exportAsCSV(table_id, separator = ',') {
                        // Select rows from table_id
                        var rows = document.querySelectorAll('#' + table_id + ' tr');
                        // Construct csv
                        var csv = [];
                        for (var i = 0; i < rows.length; i++) {
                            if(rows[i].style.display== "none"){
                                continue;
                            }
                            var row = [], cols = rows[i].querySelectorAll('td, th');
                            for (var j = 0; j < cols.length; j++) {
                                // Clean innertext to remove multiple spaces and jumpline (break csv)
                                var data = cols[j].innerText.replace(/(\\r\\n|\\n|\\r)/gm, '').replace(/(\\s\\s)/gm, ' ')
                                    // Escape double-quote with double-double-quote (see https://stackoverflow.com/questions/17808511/properly-escape-a-double-quote-in-csv)
                                    data = data.replace(/"/g, '""');
                                // Push escaped string
                                row.push('"' + data + '"');
                            }
                            csv.push(row.join(separator));
                        }
                        var csv_string = csv.join('\\n');
                        // Download it
                        var filename = 'export_' + table_id + '_' + new Date().toLocaleDateString() + '.csv';
                        var link = document.createElement('a');
                        link.style.display = 'none';
                        link.setAttribute('target', '_blank');
                        link.setAttribute('href', 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv_string));
                        link.setAttribute('download', filename);
                        document.body.appendChild(link);
                        link.click();
                        document.body.removeChild(link);
                    }
                """;
  }

  public String getFilterTableText() {
    return """
                    function filterTable(tableID, input, rowsTextSpan) {
                        // Declare variables
                        var input, filter, table, tr, td, i, txtValue;
                        input = document.getElementById(input);
                        rowsText = document.getElementById(rowsTextSpan);
                        filter = input.value.toUpperCase();
                        table = document.getElementById(tableID);
                        tr = table.rows;

                        let count = -1;//it is -1 to skip the header
                                       // Loop through all table rows, and hide those who don't match the search query
                        for (i = 0; i < tr.length; i++) {
                            tds = tr[i].getElementsByTagName("td");
                            var matches = false;
                            var validRow = false;
                            for (y = 0; y < tds.length; y++) {
                                td = tds[y];
                                if (td) {
                                    validRow = true
                                        txtValue = td.textContent || td.innerText;
                                    if (isNaN){
                                        matches = txtValue.toUpperCase().indexOf(filter) > -1;
                                        if (matches){
                                            break;
                                        }
                                    }
                                }
                            }
                            if (!matches && validRow){
                                tr[i].style.display = "none";
                            } else {
                                count++
                                    tr[i].style.display = "";
                            }
                        }
                        if (count != 1){
                            rowsText.innerText= " " + count + " rows shown";
                        } else {
                            rowsText.innerText= " " + count + " row shown";
                        }
                    }
                """;
  }
}
