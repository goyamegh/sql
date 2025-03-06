package org.opensearch.sql.directquery.rest.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class BaseDirectQueryRequest {
  // Required fields
  private String dataSources;     // Required: From URI path parameter or request body
  private String sourceVersion;  // Required: API version

  // Session management
  private String sessionId;      // For session management
}
