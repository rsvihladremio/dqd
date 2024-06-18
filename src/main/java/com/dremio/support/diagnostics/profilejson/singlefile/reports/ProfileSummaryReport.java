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
package com.dremio.support.diagnostics.profilejson.singlefile.reports;

import com.dremio.support.diagnostics.profilejson.plan.PlanRelation;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.BlockReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.FindingsReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.MemoryUsed;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.MemoryUsedPerNode;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.NonDefaultKeysReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.OperatorsRecordsScannedReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.PlanDetailsReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.RowEstimateReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.StateTimingsReport;
import com.dremio.support.diagnostics.profilejson.singlefile.reports.summary.TopLineProfileSummary;
import com.dremio.support.diagnostics.shared.dto.profilejson.ProfileJSON;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ProfileSummaryReport {

  public String generateSummary(
      final boolean showPlanDetails,
      final ProfileJSON parsed,
      final Collection<PlanRelation> relations) {
    final StringBuilder builder = new StringBuilder();

    final List<ProfileJSONReport> reports =
        Arrays.asList(
            new TopLineProfileSummary(),
            new FindingsReport(),
            new NonDefaultKeysReport(),
            new StateTimingsReport(),
            new OperatorsRecordsScannedReport(),
            new MemoryUsed(),
            new MemoryUsedPerNode(),
            new RowEstimateReport(),
            new BlockReport());
    for (final ProfileJSONReport report : reports) {
      builder.append(report.generateReport(parsed, relations));
    }
    if (showPlanDetails) {
      builder.append(new PlanDetailsReport().generateReport(parsed, relations));
    }
    return builder.toString();
  }
}
