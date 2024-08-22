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

/**
 * Operator represents an operator in Dremio (or an operatorProfile in a profile.json).
 * There are a lot of helper methods on this such as createFromOperatorProfile which do the
 * important steps for object creation.
 *
 * This can be converted to a record now that jdk 17 is required to build DQD
 */
public class Operator {

  /**
   * should be the only way an Operator object is created
   *
   * @param operatorProfile the object parsed directly out of the profiles.json
   * @param metricsDef the magic map that matches the metrics id to the metrics name
   * @return an Operator instance
   */
  public static Operator createFromOperatorProfile(
      final OperatorProfile operatorProfile, final List<MetricsDef> metricsDef) {
    final Operator operator = new Operator();
    operator.setId(operatorProfile.getOperatorId());
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
    final List<MetricDef> definitions;
    // protect against null metricsDef or invalid operator types
    if (metricsDef != null
        && operatorProfile.getOperatorType() >= 0
        && metricsDef.size() > operatorProfile.getOperatorType()) {
      final MetricsDef defNested1 = metricsDef.get(operatorProfile.getOperatorType());
      definitions = defNested1.getMetricDef();
    } else {
      definitions = new ArrayList<>();
    }
    //  get the metrics for the operator profile
    final List<Metric> metrics = operatorProfile.getMetric();
    if (metrics != null) {
      for (final Metric metric : metrics) {
        final long metricId = metric.getMetricId();
        final long metricValue = metric.getLongValue();
        final OperatorMetric operatorMetric = new OperatorMetric();
        operatorMetric.setMetricId(metricId);
        // map the operator from the metric definitions
        for (final MetricDef def : definitions) {
          if (def.getId() == metricId) {
            // if there is a match go ahead and set the id
            // TODO we should probably break when it's found
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

  /**
   * helper method used to convert from nanos
   * @param nanos number to convert
   * @return milliseconds
   */
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
  private long threadId;
  private List<OperatorMetric> allMetrics;

  /**
   * decimal format to use to make numbers more readable
   */
  private final DecimalFormat df = new DecimalFormat("###,###,###");

  /**
   * gets process time for operator
   * @return total process time in nanos
   */
  public long getProcessTimeNanos() {
    return processTimeNanos;
  }

  /**
   * gets peak memory allocated by this operator instance
   * @return peak memory allocated in bytes
   */
  public long getPeakMemoryAllocated() {
    return peakMemoryAllocated;
  }

  /**
   * sets peak memory allocated by this operator instance
   * @param peakMemoryAllocated peak memory allocated in bytes
   */
  public void setPeakMemoryAllocated(final long peakMemoryAllocated) {
    this.peakMemoryAllocated = peakMemoryAllocated;
  }

  /**
   * helper method that returns the records per seconds processed of this operator instance
   * @return records per second rounded to the nearest long
   */
  public long getRecordsPerSecond() {
    final double rateMillis = this.records / (double) this.getProcessTimeMillis();
    final double rateSeconds = rateMillis * 1000;
    return Math.round(rateSeconds);
  }

  /**
   * sets the process time
   * @param processTimeNanos process of the operator instance in nanos
   */
  public void setProcessTimeNanos(final long processTimeNanos) {
    this.processTimeNanos = processTimeNanos;
  }

  /**
   * gets the process time
   * @return process time of the operator instance in nanos
   */
  public long getProcessTimeMillis() {
    return nanosToMillis(this.processTimeNanos);
  }

  /**
   * gets the wait time
   * @return wait time in millis
   */
  public long getWaitMillis() {
    return waitMillis;
  }

  /**
   * sets the wait time
   * @param waitMillis wait time in millis
   */
  public void setWaitMillis(final long waitMillis) {
    this.waitMillis = waitMillis;
  }

  /**
   * get the setup time for the operator
   *
   * @return setup time in milliseconds
   */
  public long getSetupMillis() {
    return setupMillis;
  }

  /**
   * set the setup time for the operator
   * @param setupMillis the setup time in milliseconds
   */
  public void setSetupMillis(final long setupMillis) {
    this.setupMillis = setupMillis;
  }

  /**
   * gets how many batches were in the operator
   *
   * @return the number of batches in the operator
   */
  public long getBatches() {
    return batches;
  }

  /**
   * sets how many batches were in the operator
   *
   * @param batches number of batches used in the operator
   */
  public void setBatches(final long batches) {
    this.batches = batches;
  }

  /**
   * gets number of records processed in the operator
   *
   * @return total number of records processed
   */
  public long getRecords() {
    return records;
  }

  /**
   * sets number of records processed in the operator
   *
   * @param records total number of records processed
   */
  public void setRecords(final long records) {
    this.records = records;
  }

  /**
   * gets the actual id of the operator in Dremio and in the operator profile
   *
   * @return operator id
   */
  public long getId() {
    return id;
  }

  /**
   * sets the actual id of the operator in Dremio and in the operator profile
   *
   * @param id actual id of the operator in the Dremio UI
   */
  public void setId(final long id) {
    this.id = id;
  }

  /**
   * maps to the enum CoreOperatorType, this is a friendly name
   *
   * @return CoreOperatorType name ex BROADCAST_SENDER
   */
  public String getKind() {
    return kind;
  }

  /**
   * maps to the enum CoreOperatorType, this is a friendly name
   *
   * @param kind CoreOperatorType name ex BROADCAST_SENDER
   */
  public void setKind(final String kind) {
    this.kind = kind;
  }

  /**
   *
   * maps to major fragment id in the operatorprofile and phase in the Dremio UI
   *
   * @return the phase id of the operator
   */
  public int getParentPhaseId() {
    return parentPhaseId;
  }

  /**
   * maps to major fragment id in the operatorprofile and phase in the Dremio UI
   *
   * @param parentPhaseId set the phase id of the operator
   */
  public void setParentPhaseId(final int parentPhaseId) {
    this.parentPhaseId = parentPhaseId;
  }

  /**
   * helper method that gets the total operator run time in millis
   *
   * @return combination of process time, wait time and setup time
   */
  public long getTotalTimeMillis() {
    return this.getProcessTimeMillis() + this.getWaitMillis() + this.getSetupMillis();
  }

  /**
   * setter for operator metrics
   *
   * @param allMetrics operator metric list
   */
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

  /**
   * thread id of the operator (ties to minor fragment id in Operator Profile)
   * @return minor fragment id of the operator
   */
  public long getThreadId() {
    return this.threadId;
  }

  /**
   * thread id of the operator (ties to minor fragment id in Operator Profile)
   *
   * @param threadId minor fragment id of the operator
   */
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
