package org.opensearch.sql.spark.rest.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExecuteDirectQueryResponse {
  private String queryId;
  private String result;
  private String sessionId;
}
