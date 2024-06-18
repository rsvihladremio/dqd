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

public enum QueryState {
  STARTING, // 0 => query has been scheduled for execution. This is post-enqueued.
  RUNNING, // 1 =>
  COMPLETED, // 2 => query has completed successfully
  CANCELED, // 3 => query has been cancelled, and all cleanup is complete
  FAILED, // 4 =>
  NO_LONGER_USED_1, // 5 => formerly meant cancellation requested, no longer used.
  ENQUEUED, // 6 => query has been enqueued. this is pre-starting.
  // UNKNOWN, // _ =>
}
