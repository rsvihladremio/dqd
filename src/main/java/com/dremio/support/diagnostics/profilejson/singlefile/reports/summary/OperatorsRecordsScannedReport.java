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
package com.dremio.support.diagnostics.profilejson.singlefile.reports.summary;

import static com.dremio.support.diagnostics.shared.HtmlTableDataColumn.col;

import com.dremio.support.diagnostics.profilejson.CoreOperatorType;
import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.ProfileJSONReport;
import com.dremio.support.diagnostics.shared.HtmlTableBuilder;
import com.dremio.support.diagnostics.shared.HtmlTableDataColumn;
import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.FragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.InputProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.MinorFragmentProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.OperatorProfile;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class OperatorsRecordsScannedReport extends ProfileJSONReport {

  public static class OperatorRecordsScannedReportResult {
    private List<OperatorRecordDetail> top10RecordsScanned;
    private List<OperatorRecordDetail> top10SlowestScanned;
    private List<OperatorRecordDetail> top10SlowestScannedRate;
    private List<OperatorRecordDetail> top10MostPeakMemory;

    public List<OperatorRecordDetail> getTop10RecordsScanned() {
      return top10RecordsScanned;
    }

    public void setTop10RecordsScanned(List<OperatorRecordDetail> top10RecordsScanned) {
      this.top10RecordsScanned = top10RecordsScanned;
    }

    public List<OperatorRecordDetail> getTop10SlowestScanned() {
      return top10SlowestScanned;
    }

    public void setTop10SlowestScanned(List<OperatorRecordDetail> top10SlowestScanned) {
      this.top10SlowestScanned = top10SlowestScanned;
    }

    public List<OperatorRecordDetail> getTop10SlowestScannedRate() {
      return top10SlowestScannedRate;
    }

    public void setTop10SlowestScannedRate(List<OperatorRecordDetail> top10SlowestScannedRate) {
      this.top10SlowestScannedRate = top10SlowestScannedRate;
    }

    public boolean isEmpty() {
      final int top10RecordsScanned;
      if (this.top10RecordsScanned != null) {
        top10RecordsScanned = this.top10RecordsScanned.size();
      } else {
        top10RecordsScanned = 0;
      }
      final int top10SlowestScanned;
      if (this.top10RecordsScanned != null) {
        top10SlowestScanned = this.top10SlowestScanned.size();
      } else {
        top10SlowestScanned = 0;
      }
      final int top10SlowestScannedRate;
      if (this.top10RecordsScanned != null) {
        top10SlowestScannedRate = this.top10SlowestScanned.size();
      } else {
        top10SlowestScannedRate = 0;
      }
      final int top10MostPeakMemory;
      if (this.top10MostPeakMemory != null) {
        top10MostPeakMemory = this.top10MostPeakMemory.size();
      } else {
        top10MostPeakMemory = 0;
      }
      return (top10RecordsScanned
              + top10SlowestScanned
              + top10SlowestScannedRate
              + top10MostPeakMemory)
          == 0;
    }

    public void setTop10MostPeakMemory(List<OperatorRecordDetail> top10MostPeakMemory) {
      this.top10MostPeakMemory = top10MostPeakMemory;
    }

    public List<OperatorRecordDetail> getTop10MostPeakMemory() {
      return top10MostPeakMemory;
    }
  }

  public static OperatorRecordsScannedReportResult generateRecordReport(
      ProfileJSON profileJson, Collection<PlanRelation> relations) {
    OperatorRecordsScannedReportResult result = new OperatorRecordsScannedReportResult();
    if (profileJson == null) {
      return result;
    }
    List<FragmentProfile> fragmentProfiles = profileJson.getFragmentProfile();
    if (fragmentProfiles == null) {
      return result;
    }
    final int processors = Math.max(Runtime.getRuntime().availableProcessors() - 1, 2);
    final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(processors);
    final List<Future<?>> futures = new ArrayList<>();
    final Lock operatorListLock = new ReentrantLock();
    final List<OperatorRecordDetail> operators = new ArrayList<>();
    for (final FragmentProfile fragmentProfile : fragmentProfiles) {
      final List<MinorFragmentProfile> minorFragmentProfiles =
          fragmentProfile.getMinorFragmentProfile();
      if (minorFragmentProfiles == null) {
        continue;
      }
      for (final MinorFragmentProfile minorFragmentProfile : minorFragmentProfiles) {
        final List<OperatorProfile> operatorProfiles = minorFragmentProfile.getOperatorProfile();
        if (operatorProfiles == null) {
          continue;
        }
        for (final OperatorProfile operatorProfile : operatorProfiles) {
          final CoreOperatorType operatorType =
              CoreOperatorType.values()[operatorProfile.getOperatorType()];
          final String phaseId =
              StringUtils.leftPad(String.valueOf(fragmentProfile.getMajorFragmentId()), 2, "0");
          final String threadId =
              StringUtils.leftPad(
                  String.valueOf(minorFragmentProfile.getMinorFragmentId()), 2, "0");
          final String operatorId =
              StringUtils.leftPad(String.valueOf(operatorProfile.getOperatorId()), 2, "0");
          final List<InputProfile> inputProfiles = operatorProfile.getInputProfile();
          if (inputProfiles == null) {
            continue;
          }
          futures.add(
              threadPoolExecutor.submit(
                  () -> {
                    long records = 0;
                    long batches = 0;
                    for (final InputProfile inputProfile : inputProfiles) {
                      if (inputProfile == null) {
                        continue;
                      }
                      records += inputProfile.getRecords();
                      batches += inputProfile.getBatches();
                    }
                    OperatorRecordDetail detail = new OperatorRecordDetail();

                    detail.setName(
                        String.format(
                            "%s-%s-%s %s", phaseId, threadId, operatorId, operatorType.toString()));
                    detail.setBatches(batches);
                    detail.setRecords(records);
                    detail.setRunTimeNanos(operatorProfile.getProcessNanos());
                    detail.setPeakLocalMemoryAllocated(
                        operatorProfile.getPeakLocalMemoryAllocated());
                    for (PlanRelation planRelation : relations) {
                      String phaseOperatorName = String.format("%s-%s", phaseId, operatorId);
                      if (planRelation.getName().equals(phaseOperatorName)) {
                        if (planRelation.getValues().containsKey("condition")) {
                          detail.setCondition(planRelation.getValues().get("condition").toString());
                          break;
                        }
                      }
                    }
                    operatorListLock.lock();
                    try {
                      operators.add(detail);
                    } finally {
                      operatorListLock.unlock();
                    }
                  }));
        }
      }
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      futures.add(
          threadPoolExecutor.submit(
              () -> {
                final List<OperatorRecordDetail> top10Records =
                    operators.parallelStream()
                        .sorted(
                            (left, right) -> Long.compare(right.getRecords(), left.getRecords()))
                        .limit(10)
                        .collect(Collectors.toList());
                result.setTop10RecordsScanned(top10Records);
              }));
      futures.add(
          threadPoolExecutor.submit(
              () -> {
                final List<OperatorRecordDetail> top10Slowest =
                    operators.parallelStream()
                        .sorted(
                            (left, right) ->
                                Long.compare(right.getRunTimeNanos(), left.getRunTimeNanos()))
                        .limit(10)
                        .collect(Collectors.toList());
                result.setTop10SlowestScanned(top10Slowest);
              }));
      futures.add(
          threadPoolExecutor.submit(
              () -> {
                final List<OperatorRecordDetail> top10SlowestRates =
                    operators.parallelStream()
                        .filter(x -> x.getRecords() > 0)
                        .sorted(
                            Comparator.comparingDouble(OperatorRecordDetail::getRecordsPerSecond))
                        .limit(10)
                        .collect(Collectors.toList());
                result.setTop10SlowestScannedRate(top10SlowestRates);
              }));
      futures.add(
          threadPoolExecutor.submit(
              () -> {
                final List<OperatorRecordDetail> top10MostPeakMemory =
                    operators.parallelStream()
                        .filter(x -> x.getPeakLocalMemoryAllocated() > 0)
                        .sorted(
                            (left, right) ->
                                Long.compare(
                                    right.getPeakLocalMemoryAllocated(),
                                    left.getPeakLocalMemoryAllocated()))
                        .limit(10)
                        .collect(Collectors.toList());
                result.setTop10MostPeakMemory(top10MostPeakMemory);
              }));
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return result;
  }

  @Override
  protected String createReport(ProfileJSON profileJson, Collection<PlanRelation> relations) {

    final OperatorRecordsScannedReportResult report =
        OperatorsRecordsScannedReport.generateRecordReport(profileJson, relations);
    final HtmlTableBuilder htmlTableBuilder = new HtmlTableBuilder();

    final StringBuilder builder = new StringBuilder();
    if (report.isEmpty()) {
      builder.append("<h2>Operator Record Reports</h2>\n");
      builder.append("<p>No records to analyze</p>\n");
    } else {
      Collection<Collection<HtmlTableDataColumn<String, Number>>> operatorsWithMostRecordsRows =
          new ArrayList<>();
      for (final OperatorRecordDetail detail : report.getTop10RecordsScanned()) {
        List<HtmlTableDataColumn<String, Number>> row =
            Arrays.asList(
                HtmlTableDataColumn.col(detail.getName()),
                col(
                    String.format("%s", Human.getHumanDurationFromNanos(detail.getRunTimeNanos())),
                    detail.getRunTimeNanos()),
                col(String.format("%,d", detail.getRecords()), detail.getRecords()),
                col(String.format("%,d", detail.getBatches()), detail.getBatches()),
                col(
                    String.format("%,.2f", detail.getRecordsPerSecond()),
                    detail.getRecordsPerSecond()),
                col(
                    Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                    detail.getPeakLocalMemoryAllocated()),
                HtmlTableDataColumn.col(detail.getCondition()));
        operatorsWithMostRecordsRows.add(row);
      }
      builder.append(
          htmlTableBuilder.generateTable(
              "operatorWithMostRecords",
              "Operators with Most Records",
              Arrays.asList(
                  "name",
                  "process time",
                  "records",
                  "batches",
                  "records per second",
                  "peak memory",
                  "conditions"),
              operatorsWithMostRecordsRows));
      Collection<Collection<HtmlTableDataColumn<String, Number>>> slowestOperatorRows =
          new ArrayList<>();
      for (final OperatorRecordDetail detail : report.getTop10SlowestScanned()) {
        slowestOperatorRows.add(
            Arrays.asList(
                HtmlTableDataColumn.col(detail.getName()),
                col(
                    String.format("%s", Human.getHumanDurationFromNanos(detail.getRunTimeNanos())),
                    detail.getRunTimeNanos()),
                col(String.format("%,d", detail.getRecords()), detail.getRecords()),
                col(String.format("%,d", detail.getBatches()), detail.getBatches()),
                col(
                    String.format("%,.2f", detail.getRecordsPerSecond()),
                    detail.getRecordsPerSecond()),
                col(
                    Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                    detail.getPeakLocalMemoryAllocated()),
                HtmlTableDataColumn.col(detail.getCondition())));
      }
      builder.append(
          htmlTableBuilder.generateTable(
              "slowestOperatorsTable",
              "Slowest Operators",
              Arrays.asList(
                  "name",
                  "process time",
                  "records",
                  "batches",
                  "records per second",
                  "peak memory",
                  "conditions"),
              slowestOperatorRows));
      Collection<Collection<HtmlTableDataColumn<String, Number>>> slowestScanRateRows =
          new ArrayList<>();
      for (final OperatorRecordDetail detail : report.getTop10SlowestScannedRate()) {
        slowestScanRateRows.add(
            Arrays.asList(
                HtmlTableDataColumn.col(detail.getName()),
                col(
                    String.format("%s", Human.getHumanDurationFromNanos(detail.getRunTimeNanos())),
                    detail.getRunTimeNanos()),
                col(String.format("%,d", detail.getRecords()), detail.getRecords()),
                col(String.format("%,d", detail.getBatches()), detail.getBatches()),
                col(
                    String.format("%,.2f", detail.getRecordsPerSecond()),
                    detail.getRecordsPerSecond()),
                col(
                    Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                    detail.getPeakLocalMemoryAllocated()),
                HtmlTableDataColumn.col(detail.getCondition())));
      }
      builder.append(
          htmlTableBuilder.generateTable(
              "slowestOperatorsByRecordsPerSecondTable",
              "Slowest Operators by Records Per Second",
              Arrays.asList(
                  "name",
                  "process time",
                  "records",
                  "batches",
                  "records per second",
                  "peak memory",
                  "conditions"),
              slowestScanRateRows));
      final Collection<Collection<HtmlTableDataColumn<String, Number>>> peakMemoryUsedRows =
          new ArrayList<>();
      for (final OperatorRecordDetail detail : report.getTop10MostPeakMemory()) {
        peakMemoryUsedRows.add(
            Arrays.asList(
                HtmlTableDataColumn.col(detail.getName()),
                col(
                    String.format("%s", Human.getHumanDurationFromNanos(detail.getRunTimeNanos())),
                    detail.getRunTimeNanos()),
                col(String.format("%,d", detail.getRecords()), detail.getRecords()),
                col(String.format("%,d", detail.getBatches()), detail.getBatches()),
                col(
                    String.format("%,.2f", detail.getRecordsPerSecond()),
                    detail.getRecordsPerSecond()),
                col(
                    Human.getHumanBytes1024(detail.getPeakLocalMemoryAllocated()),
                    detail.getPeakLocalMemoryAllocated()),
                HtmlTableDataColumn.col(detail.getCondition())));
      }
      builder.append(
          htmlTableBuilder.generateTable(
              "mostPeakMemoryUsedPerPhase",
              "Most Peak Memory Used Per Phase (Across All Nodes)",
              Arrays.asList(
                  "name",
                  "process time",
                  "records",
                  "batches",
                  "records per second",
                  "peak memory",
                  "conditions"),
              peakMemoryUsedRows));
    }
    return builder.toString();
  }

  @Override
  public String htmlSectionName() {
    return "operators-scanned-section";
  }

  @Override
  public String htmlTitle() {
    return "Operators";
  }
}
