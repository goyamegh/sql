package org.opensearch.sql.directquery.rest.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
public class GetDirectQueryResourcesRequest extends BaseDirectQueryRequest {
  String resourceType;
  String resourceName;

  // Optional fields
  private Map<String, String> queryParams;
}
