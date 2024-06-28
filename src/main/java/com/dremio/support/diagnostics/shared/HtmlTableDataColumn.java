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

public record HtmlTableDataColumn<D, S>(D data, S sortableData, boolean limitText) {

  public static <D, S> HtmlTableDataColumn<D, S> col(D data) {
    return new HtmlTableDataColumn<D, S>(data, null, false);
  }

  public static <D, S> HtmlTableDataColumn<D, S> col(D data, boolean limitText) {
    return new HtmlTableDataColumn<D, S>(data, null, limitText);
  }

  public static <D, S> HtmlTableDataColumn<D, S> col(D data, S sortableData) {
    return new HtmlTableDataColumn<D, S>(data, sortableData, false);
  }

  public static <D, S> HtmlTableDataColumn<D, S> col(D data, S sortableData, boolean limitText) {
    return new HtmlTableDataColumn<D, S>(data, sortableData, limitText);
  }
}
