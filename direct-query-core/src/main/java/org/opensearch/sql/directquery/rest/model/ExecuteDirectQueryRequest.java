/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ExecuteDirectQueryRequest {
  private String datasource;
  private String query;
  private String queryType;
  private String startTime;
  private String endTime;
  private String step;
  private String sessionId;
  private String queryId;
}
