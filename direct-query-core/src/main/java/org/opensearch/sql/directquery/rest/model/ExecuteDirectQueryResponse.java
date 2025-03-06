package org.opensearch.sql.directquery.rest.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExecuteDirectQueryResponse {
  private String queryId;
  private String result;
  private String sessionId;
}
