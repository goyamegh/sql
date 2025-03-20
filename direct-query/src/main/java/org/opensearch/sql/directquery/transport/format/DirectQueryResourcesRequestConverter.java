/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.format;

import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;

@UtilityClass
public class DirectQueryResourcesRequestConverter {

  /**
   * Converts a RestRequest to a GetDirectQueryResourcesRequest.
   *
   * @param restRequest The REST request to convert
   * @return A configured GetDirectQueryResourcesRequest
   */
  public static GetDirectQueryResourcesRequest fromRestRequest(RestRequest restRequest) {
    GetDirectQueryResourcesRequest directQueryRequest = new GetDirectQueryResourcesRequest();
    directQueryRequest.setDataSource(restRequest.param("dataSource"));

    String path = restRequest.path();
    if (path.contains("/alertmanager/api/v2/")) {
      // Handle Alertmanager API endpoints
      if (path.contains("/alerts/groups")) {
        directQueryRequest.setResourceType("alertmanager_alert_groups");
      } else if (path.contains("/alerts")) {
        directQueryRequest.setResourceType("alertmanager_alerts");
      } else if (path.contains("/receivers")) {
        directQueryRequest.setResourceType("alertmanager_receivers");
      } else if (path.contains("/silences")) {
        directQueryRequest.setResourceType("alertmanager_silences");
      }
    } else {
      // Handle standard API endpoints
      directQueryRequest.setResourceType(restRequest.param("resourceType"));
      if (restRequest.param("resourceName") != null) {
        directQueryRequest.setResourceName(restRequest.param("resourceName"));
      }
    }

    directQueryRequest.setQueryParams(
        restRequest.params().keySet().stream()
            .filter(p -> !restRequest.consumedParams().contains(p))
            .collect(Collectors.toMap(p -> p, restRequest::param)));

    return directQueryRequest;
  }
}
