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
package com.dremio.support.diagnostics.repro.fileout;

import com.dremio.support.diagnostics.shared.FileMaker;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileOutput extends FileOutput {
  private final ZipOutputStream zipOutputStream;

  /**
   * @param zipOutputStream in memory zip reference to write all sql files and the script that runs
   *     ingest
   * @param timeoutSeconds timeout to give for creation of pds and vds
   * @param fileMaker generator for source directories
   */
  public ZipFileOutput(
      final ZipOutputStream zipOutputStream, final int timeoutSeconds, final FileMaker fileMaker) {
    super(timeoutSeconds, fileMaker);
    this.zipOutputStream = zipOutputStream;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If the stream is
   * already closed then invoking this method has no effect.
   *
   * <p>As noted in {@link AutoCloseable#close()}, cases where the close may fail require careful
   * attention. It is strongly advised to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    super.close();
    zipOutputStream.flush();
  }

  @Override
  protected void writeFile(String fileName, byte[] data) throws IOException {
    ZipEntry zipEntry = new ZipEntry(fileName);
    zipOutputStream.putNextEntry(zipEntry);
    zipOutputStream.write(data);
    zipOutputStream.closeEntry();
  }

  /**
   * provides a title so that consumers can know which strategy is running
   *
   * @return provides a title so that consumers can know which strategy is running
   */
  @Override
  public String getName() {
    return "generated zip with create.sh script";
  }
}
