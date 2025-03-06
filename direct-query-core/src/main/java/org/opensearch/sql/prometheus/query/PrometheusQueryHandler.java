/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.query;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.opensearch.security.SecurityAccess;

public class PrometheusQueryHandler implements QueryHandler {
  private static final Logger LOG =
      LogManager.getLogger(PrometheusQueryHandler.class);

  @Override
  public DataSourceType getSupportedDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }
  
  @Override
  public boolean canHandle(Object client) {
    return client instanceof PrometheusClient;
  }
  
  @Override
  public String executeQuery(Object client, ExecuteDirectQueryRequest request) throws IOException {
    return SecurityAccess.doPrivileged(() -> {
      try {

        PrometheusOptions options = request.getPrometheusOptions();
        String startTimeStr = options.getStart();
        String endTimeStr = options.getEnd();
        
        if (startTimeStr == null || endTimeStr == null) {
          return "{\"error\": \"Start and end times are required for Prometheus queries\"}";
        }
        
        if (options.getQueryType() == PrometheusQueryType.RANGE) {
          
          JSONObject metricData = ((PrometheusClient) client).queryRange(
              request.getQuery(),
              Long.parseLong(startTimeStr),
              Long.parseLong(endTimeStr),
              options.getStep());
          return metricData.toString();
        }
        return "{\"error\": \"Invalid query type: " + options.getQueryType().toString() + "\"}";
      } catch (NumberFormatException e) {
        return "{\"error\": \"Invalid time format: " + e.getMessage() + "\"}";
      } catch (IOException e) {
        LOG.error("Error executing query", e);
        return "{\"error\": \"" + e.getMessage() + "\"}";
      }
    });
  }
}