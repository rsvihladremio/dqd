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

import com.github.freva.asciitable.AsciiTable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

public abstract class TextRowBaseReport implements Report {
  private final String[][] rows;
  private final String[] header;

  /**
   * The constructor clones the arrays and passes them on instead of using original references.
   *
   * @param rows the rows to output to text using ascii borders
   * @param header the header fields of the ascii table
   */
  public TextRowBaseReport(String[][] rows, String... header) {
    if (rows == null) {
      throw new InvalidParameterException("report rows cannot be null");
    }
    if (header == null) {
      throw new InvalidParameterException("report header cannot be null");
    }
    this.rows = rows.clone();
    this.header = header.clone();
  }

  /**
   * Output rows and header to an ascii table that is nicely formatted for a console
   *
   * @return the acsii table text as a string
   */
  @Override
  public String getText() {
    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
      AsciiTable.builder().header(this.header).data(this.rows).writeTo(stream);
      return stream.toString("UTF-8");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** The implementer chooses the title of the report */
  @Override
  public abstract String getTitle();
}
