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
package com.dremio.support.diagnostics.queriesjson;

import com.dremio.support.diagnostics.queriesjson.filters.DateRangeQueryFilter;
import com.dremio.support.diagnostics.queriesjson.reporters.QueryReporter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;

/**
 * Parses zip and tgz files for queries.json files either unzipped or in a gzipped format
 * (the original format Dremio stores the files in)
 */
public class ReadArchive {

  /**
   * standard logging object
   */
  private static final Logger LOGGER = Logger.getLogger(ReadArchive.class.getName());

  /**
   * proivdes the filtering of dates so that we do not see data outside of the range requested
   */
  private final DateRangeQueryFilter dateFilter;

  /**
   * Parses zip and tgz files for queries.json files either unzipped or in a gzipped format
   * (the original format Dremio stores the files in)
   * @param dateFilter limits the queries that show up in the report. The filter is based on start epoch of the timestamp
   */
  public ReadArchive(final DateRangeQueryFilter dateFilter) {
    this.dateFilter = dateFilter;
  }

  /**
   * the logic to parse a gzip directly (skip extracting to disk)
   *
   * @param fileName original archive entry name used for reporting purposes only
   * @param source the location of the file that we will parse
   * @param reports list of reporters to run against each query
   * @throws IOException when we're unable to read the gzip file
   */
  public SearchedFile parseGzip(String fileName, Path source, Collection<QueryReporter> reports)
      throws IOException {
    try (var tmpFileStream = Files.newInputStream(source)) {
      GZIPInputStream gzis;
      try {
        gzis = new GZIPInputStream(tmpFileStream);
        return QueriesJsonFileParser.parseFile(fileName, gzis, reports, dateFilter);
      } catch (ZipException ex) {
        // not a valid gzip so no reason to continue
        LOGGER.warning("invalid gzip skipping entry %s".formatted(fileName));
        return new SearchedFile(0, 0, fileName, ex.getMessage());
        // } catch (Exception ex) {
        //   LOGGER.log(Level.WARNING, "unhandled exception: processing entry
        // %s".formatted(fileName), ex);
        //   return new SearchedFile(0, 0, fileName, ex.getMessage());
      }
    }
  }

  /**
   *
   * @param fileName original archive entry name used for reporting purposes only
   * @param source the location of the file that we will parse
   * @param reports list of reporters to run against each query
   * @throws IOException when we're unable to read the text file
   */
  private SearchedFile parseJSON(String fileName, Path source, Collection<QueryReporter> reports)
      throws IOException {
    try (var tmpFileStream = Files.newInputStream(source)) {
      return QueriesJsonFileParser.parseFile(fileName, tmpFileStream, reports, dateFilter);
    }
  }

  /**
   * Catch all method that drives the archive parsing logic and iterates through all the entries via the following approach:
   * - it does not extract the archive, but only reads the entries inside
   * - if it does find an entry that we want to parse, that file is then extracted to a temp file and deleted after parsing is done
   * - each file that is parsed is done so from a thread pool.
   * - each query that is parsed is visited by a list of reporters and not kept and therefore can be released immediately
   *
   * this gives us several useful properties
   * - each CPU thread tends to stay busy at all times
   * - it does not use more disk space than (number of threads * individual queries.json file size)
   *   so if each file is 10mb and there are 8 threads, then 80mb of disk space will be used
   * - it does not use more memory that (number of threads * individual query size)
   *   so if each query is 1mb and there are 8 threads, then 8mb fo ram will be consumed by this method (though reporters may consume more)
   *
   * @param is archive containing queries.json to reach from
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @return files that were searched in the archive
   * @throws JsonMappingException from jackson if there is an invalid row
   * @throws JsonProcessingException from jackon
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  private Collection<SearchedFile> parse(
      @SuppressWarnings("rawtypes") final ArchiveInputStream is,
      final Collection<QueryReporter> reporters,
      final int threads)
      throws JsonMappingException,
          JsonProcessingException,
          IOException,
          InterruptedException,
          ExecutionException {

    final ExecutorService executorService = Executors.newFixedThreadPool(threads);
    final List<Future<?>> futures = new ArrayList<>();
    final List<SearchedFile> entries = new ArrayList<>();
    ArchiveEntry entry;
    while (null != (entry = is.getNextEntry())) {
      // Check if entry is a directory
      if (!entry.isDirectory()) {
        // only attempt to process files with "queries" in the name
        if (entry.getName().contains("queries")) {
          // gzips need to be handled with a different code path
          final String entryName = entry.getName();
          final boolean isMaybeGZip = entryName.endsWith("gz");
          final boolean isBzip2 = entryName.endsWith("bzip2");
          final boolean isJson = entryName.endsWith("json");
          // only parse files gzips and json files
          if (isJson || isMaybeGZip || isBzip2) {
            final Path tmpFile = Files.createTempFile("oa-", "-ta");
            // this is probably hacky but I've not yet figured out a way to write a temp file in
            // java and
            // get the stream back.
            Files.copy(is, tmpFile, StandardCopyOption.REPLACE_EXISTING);
            final boolean isGzip = isValidGzip(tmpFile.toFile());
            // check to see if the file is too small to have anything meaningful inside
            final long size = tmpFile.toFile().length();
            if (size < 8) {
              // if too small skip it
              LOGGER.warning(
                  "found file of only %d bytes, not usable. Skipping entry %s"
                      .formatted(size, entryName));
              return entries;
            }
            final String fileName = entry.getName();
            // use the thread pool to run the parsing, this allows faster throughput and uses
            // more of the machine resources
            futures.add(
                executorService.submit(
                    () -> {
                      try {
                        if (isGzip) {
                          entries.add(parseGzip(fileName, tmpFile, reporters));
                        } else if (isJson) {
                          entries.add(parseJSON(fileName, tmpFile, reporters));
                        } else if (isBzip2) {
                          entries.add(parseBzip2(fileName, reporters));
                        } else if (isMaybeGZip) {
                          entries.add(new SearchedFile(0, 0, fileName, ""));
                          LOGGER.finer(
                              () ->
                                  "skipped file %s as it has a gzip extension but is not a gzip"
                                      .formatted(entryName));
                        } else {
                          entries.add(new SearchedFile(0, 0, fileName, ""));
                          // since we have guarded above to only parse gzips, bzip2 and json files
                          // reaching this
                          // means we have left a critical bug in the code and allowed another
                          // file type into
                          // this code block with addressing it.
                          LOGGER.severe("this is a bug and this code should not be reached");
                        }
                      } catch (IOException | InterruptedException | ExecutionException e) {
                        entries.add(new SearchedFile(0, 0, fileName, e.getMessage()));
                        LOGGER.log(
                            Level.SEVERE,
                            "error parsing file %s: %s".formatted(fileName, e.getMessage()),
                            e);
                      } finally {
                        // cleanup tmp file when we are done processing
                        tmpFile.toFile().delete();
                        System.out.print(".");
                      }
                    }));
          }
        }
      }
    }
    // this will allow us to block all the futures we've submitted until they're done
    // thanks to this we can count on all parsing being done before the method executor shutsdown
    for (Future<?> future : futures) {
      future.get();
    }
    // this is probably not necessary but leaving it in case there is other code added later that
    // needs it.
    executorService.shutdown();
    return entries;
  }

  /**
   * logic ot read a tar.gz or tgz file
   * @param targz the gzipped tarball to read
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @return files that were searched
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  public Collection<SearchedFile> readTarGz(
      String targz, Collection<QueryReporter> reporters, int threads)
      throws IOException, InterruptedException, ExecutionException {
    try (FileInputStream st = new FileInputStream(targz)) {
      try (GZIPInputStream gzi = new GZIPInputStream(st)) {
        try (TarArchiveInputStream tarInput = new TarArchiveInputStream(gzi)) {
          return parse(tarInput, reporters, threads);
        }
      }
    }
  }

  /**
   * logic to read a tar.bzip2 file
   * @param tarBzip2 the bzip2 tarball to read
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @return files that were searched
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  public Collection<SearchedFile> readTarBzip2(
      String tarBzip2, Collection<QueryReporter> reporters, int threads)
      throws IOException, InterruptedException, ExecutionException {
    try (FileInputStream st = new FileInputStream(tarBzip2)) {
      try (BZip2CompressorInputStream xzi = new BZip2CompressorInputStream(st)) {
        try (TarArchiveInputStream tarInput = new TarArchiveInputStream(xzi)) {
          return parse(tarInput, reporters, threads);
        }
      }
    }
  }

  /**
   * logic to read a tar.xz file
   * @param tarXz the gzipped tarball to read
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @return files that were searched
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  public Collection<SearchedFile> readTarXz(
      String tarXv, Collection<QueryReporter> reporters, int threads)
      throws IOException, InterruptedException, ExecutionException {
    try (FileInputStream st = new FileInputStream(tarXv)) {
      try (XZCompressorInputStream xzi = new XZCompressorInputStream(st)) {
        try (TarArchiveInputStream tarInput = new TarArchiveInputStream(xzi)) {
          return parse(tarInput, reporters, threads);
        }
      }
    }
  }

  /**
   * logic to read a bzip2 file
   * @param bzip2 the bzip2 file to read
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @returns a searched file with the file name, number of records parsed and records filtered
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  public SearchedFile parseBzip2(String bzip2, Collection<QueryReporter> reporters)
      throws IOException, InterruptedException, ExecutionException {
    try (FileInputStream st = new FileInputStream(bzip2)) {
      try (BZip2CompressorInputStream bzi = new BZip2CompressorInputStream(st)) {
        return QueriesJsonFileParser.parseFile(bzip2, bzi, reporters, dateFilter);
      } catch (Exception ex) {
        // not a valid bzip2 so no reason to continue
        LOGGER.log(Level.WARNING, "invalid bzip2 skipping entry %s".formatted(bzip2), ex);
        return new SearchedFile(0, 0, bzip2, ex.getMessage());
      }
    }
  }

  /**
   * logic to read a tar file
   * @param tar the tarball to read
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @return files that were searched
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  public Collection<SearchedFile> readTar(
      String tar, Collection<QueryReporter> reporters, int threads)
      throws IOException, InterruptedException, ExecutionException {
    try (FileInputStream st = new FileInputStream(tar)) {
      try (TarArchiveInputStream tarInput = new TarArchiveInputStream(st)) {
        return parse(tarInput, reporters, threads);
      }
    }
  }

  /**
   * borrowed from stack overflow
   * https://stackoverflow.com/questions/30507653/how-to-check-whether-file-is-gzip-or-not-in-java
   *
   * Did this to minize the gzip errors we enconter
   * @param f file to check
   * @return is the magic byte is present
   */
  private boolean isValidGzip(File f) {
    int magic = 0;
    try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
      magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
    } catch (Throwable e) {
      LOGGER.log(Level.WARNING, e.getMessage(), e);
      return false;
    }
    return magic == GZIPInputStream.GZIP_MAGIC;
  }

  /**
   * logic to read a zip file
   *
   * @param zipFilePath the path to the zip file containing queries.json.gz or queries.json files
   * @param reporters reporters to run against each query that is parsed, they will need to be thread safe if threads > 1
   * @param threads concurrent number of files that will be parsed
   * @return files that were searched
   * @throws IOException if there is a file we cannot read or we cannot write the temp files
   * @throws InterruptedException if there is a threading problem
   * @throws ExecutionException if there is a thread pool issue
   */
  public Collection<SearchedFile> readZip(
      String zipFilePath, Collection<QueryReporter> reports, int threads)
      throws IOException, InterruptedException, ExecutionException {
    try (ZipArchiveInputStream zipFile =
        new ZipArchiveInputStream(new FileInputStream(zipFilePath))) {
      return parse(zipFile, reports, threads);
    }
  }
}
