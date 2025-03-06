/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.validator;

import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.prometheus.model.PrometheusOptions;

public class DirectQueryRequestValidator {

  /**
   * Validates a DirectQuery request.
   *
   * @param request The request to validate
   * @throws IllegalArgumentException if the request is invalid
   */
  public static void validateRequest(ExecuteDirectQueryRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("Request cannot be null");
    }
    
    if (request.getDataSources() == null || request.getDataSources().isEmpty()) {
      throw new IllegalArgumentException("Datasource is required");
    }
    
    if (request.getQuery() == null || request.getQuery().isEmpty()) {
      throw new IllegalArgumentException("Query is required");
    }
    
    if (request.getLanguage() == null) {
      throw new IllegalArgumentException("Language type is required");
    }
    
    if (request.getLanguage() == LangType.PROMQL) {
      PrometheusOptions prometheusOptions = request.getPrometheusOptions();
      
      String start = prometheusOptions.getStart();
      String end = prometheusOptions.getEnd();
      
      if (start != null && end != null) {
        try {
          long startTimestamp = Long.parseLong(start);
          long endTimestamp = Long.parseLong(end);
          if (endTimestamp <= startTimestamp) {
            throw new IllegalArgumentException("End time must be after start time");
          }
        } catch (NumberFormatException e) {
          // If timestamps are not numeric, we skip this validation
        }
      }
    }
  }
}
