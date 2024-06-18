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
package com.dremio.support.diagnostics.profilejson;

import com.dremio.support.diagnostics.shared.Human;
import com.dremio.support.diagnostics.shared.dto.profilejson.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Operator {

  public static Operator createFromOperatorProfile(
      final OperatorProfile operatorProfile, final List<MetricsDef> metricsDef) {
    final CoreOperatorType operatorType =
        CoreOperatorType.values()[operatorProfile.getOperatorType()];
    final Operator operator = new Operator();
    operator.setId(operatorProfile.getOperatorId());
    operator.setKind(operatorType.name());
    final long processNanos = operatorProfile.getProcessNanos();
    operator.setProcessTimeNanos(processNanos);
    final String kind = CoreOperatorType.values()[operatorProfile.getOperatorType()].toString();
    operator.setKind(kind);
    final long setupNanos = operatorProfile.getSetupNanos();
    operator.setSetupMillis(nanosToMillis(setupNanos));
    final long waitMillis = nanosToMillis(operatorProfile.getWaitNanos());
    operator.setWaitMillis(waitMillis);
    final long peakMemoryAllocated = operatorProfile.getPeakLocalMemoryAllocated();
    operator.setPeakMemoryAllocated(peakMemoryAllocated);
    long batches = 0;
    long records = 0;
    final List<InputProfile> inputProfiles = operatorProfile.getInputProfile();
    if (inputProfiles != null) {
      for (final InputProfile inputProfile : inputProfiles) {
        batches += inputProfile.getBatches();
        records += inputProfile.getRecords();
      }
    }
    final List<OperatorMetric> operatorMetrics = new ArrayList<>();
    List<MetricDef> definitions = new ArrayList<>();
    if (metricsDef != null
        && operatorProfile.getOperatorType() >= 0
        && metricsDef.size() > operatorProfile.getOperatorType()) {
      final MetricsDef defNested1 = metricsDef.get(operatorProfile.getOperatorType());
      definitions = defNested1.getMetricDef();
    }
    final List<Metric> metrics = operatorProfile.getMetric();
    if (metrics != null) {
      for (final Metric metric : metrics) {
        final long metricId = metric.getMetricId();
        final long metricValue = metric.getLongValue();
        final OperatorMetric operatorMetric = new OperatorMetric();
        operatorMetric.setMetricId(metricId);
        for (final MetricDef def : definitions) {
          if (def.getId() == metricId) {
            operatorMetric.setMetricName(def.getName());
          }
        }
        operatorMetric.setLongValue(metricValue);
        operatorMetrics.add(operatorMetric);
      }
    }
    operator.setAllMetrics(operatorMetrics);
    operator.setBatches(batches);
    operator.setRecords(records);

    return operator;
  }

  private static long nanosToMillis(final long nanos) {
    return nanos / 1000000;
  }

  private long id;
  private String kind;
  private int parentPhaseId;
  private long processTimeNanos;
  private long waitMillis;
  private long setupMillis;
  private long batches;
  private long records;

  private long peakMemoryAllocated;

  private List<OperatorMetric> allMetrics;

  private final DecimalFormat df = new DecimalFormat("###,###,###");

  private long threadId;

  public long getProcessTimeNanos() {
    return processTimeNanos;
  }

  public long getPeakMemoryAllocated() {
    return peakMemoryAllocated;
  }

  public void setPeakMemoryAllocated(final long peakMemoryAllocated) {
    this.peakMemoryAllocated = peakMemoryAllocated;
  }

  public long getRecordsPerSecond() {
    final double rateMillis = this.records / (double) this.getProcessTimeMillis();
    final double rateSeconds = rateMillis * 1000;
    return Math.round(rateSeconds);
  }

  public void setProcessTimeNanos(final long processTimeNanos) {
    this.processTimeNanos = processTimeNanos;
  }

  public long getProcessTimeMillis() {
    return nanosToMillis(this.processTimeNanos);
  }

  public long getWaitMillis() {
    return waitMillis;
  }

  public void setWaitMillis(final long waitMillis) {
    this.waitMillis = waitMillis;
  }

  public long getSetupMillis() {
    return setupMillis;
  }

  public void setSetupMillis(final long setupMillis) {
    this.setupMillis = setupMillis;
  }

  public long getBatches() {
    return batches;
  }

  public void setBatches(final long batches) {
    this.batches = batches;
  }

  public long getRecords() {
    return records;
  }

  public void setRecords(final long records) {
    this.records = records;
  }

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public String getKind() {
    return kind;
  }

  public void setKind(final String kind) {
    this.kind = kind;
  }

  public int getParentPhaseId() {
    return parentPhaseId;
  }

  public void setParentPhaseId(final int parentPhaseId) {
    this.parentPhaseId = parentPhaseId;
  }

  public long getTotalTimeMillis() {
    return this.getProcessTimeMillis() + this.getWaitMillis() + this.getSetupMillis();
  }

  public void setAllMetrics(final List<OperatorMetric> allMetrics) {
    this.allMetrics = allMetrics;
  }

  @Override
  public String toString() {
    final String metrics =
        allMetrics.stream()
            .sorted(Comparator.comparing(OperatorMetric::getMetricId))
            .map(OperatorMetric::toString)
            .collect(Collectors.joining("\n- "));
    return "phase-thread="
        + parentPhaseId
        + "-"
        + threadId
        + "\nkind='"
        + kind
        + "' (id "
        + id
        + ")"
        + "\nprocessTimeNanos="
        + Human.getHumanDurationFromNanos(processTimeNanos)
        + "\nwaitMillis="
        + Human.getHumanDurationFromMillis(waitMillis)
        + "\nsetupMillis="
        + Human.getHumanDurationFromMillis(setupMillis)
        + "\nbatches="
        + df.format(batches)
        + "\nrecords="
        + df.format(records)
        + "\npeakMemoryAllocated="
        + Human.getHumanBytes1024(peakMemoryAllocated)
        + "\nallMetrics\n----------\n- "
        + metrics;
  }

  public long getThreadId() {
    return this.threadId;
  }

  public void setThreadId(final long threadId) {
    this.threadId = threadId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Operator)) return false;
    Operator operator = (Operator) o;
    return id == operator.id
        && parentPhaseId == operator.parentPhaseId
        && processTimeNanos == operator.processTimeNanos
        && waitMillis == operator.waitMillis
        && setupMillis == operator.setupMillis
        && batches == operator.batches
        && records == operator.records
        && peakMemoryAllocated == operator.peakMemoryAllocated
        && threadId == operator.threadId
        && Objects.equals(kind, operator.kind)
        && Objects.equals(allMetrics, operator.allMetrics)
        && Objects.equals(df, operator.df);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        kind,
        parentPhaseId,
        processTimeNanos,
        waitMillis,
        setupMillis,
        batches,
        records,
        peakMemoryAllocated,
        allMetrics,
        df,
        threadId);
  }
}
