/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.system.model.MetricMetadata;
import org.opensearch.sql.opensearch.security.SecurityAccess;

public class PrometheusQueryHandler implements QueryHandler {
  private static final org.apache.logging.log4j.Logger LOG =
      org.apache.logging.log4j.LogManager.getLogger(PrometheusQueryHandler.class);

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
        PrometheusClient prometheusClient = (PrometheusClient) client;
        String queryType = request.getQueryType().toUpperCase();
        
        switch (queryType) {
          case "METRIC":
            try {
              long startTime = Long.parseLong(request.getStartTime());
              long endTime = Long.parseLong(request.getEndTime());
              
              JSONObject metricData = prometheusClient.queryRange(
                  request.getQuery(),
                  startTime,
                  endTime,
                  request.getStep());
              return metricData.toString();
            } catch (NumberFormatException e) {
              return "{\"error\": \"Invalid time format: " + e.getMessage() + "\"}";
            }
          case "LABEL":
            List<String> labels = prometheusClient.getLabels(request.getQuery());
            return new JSONArray(labels).toString();
          case "SERIES":
            Map<String, List<MetricMetadata>> allMetrics = prometheusClient.getAllMetrics();
            return new JSONObject(allMetrics.keySet()).toString();
          case "METRIC_METADATA":
            Map<String, List<MetricMetadata>> metricsMeta = prometheusClient.getAllMetrics();
            List<MetricMetadata> metaDataForMetric = metricsMeta.get(request.getQuery());
            return metaDataForMetric != null
                ? new JSONArray(metaDataForMetric).toString()
                : "{\"error\": \"No metadata found for this metric.\"}";
          default:
            return "{\"error\": \"Unsupported query type: " + queryType + "\"}";
        }
      } catch (IOException e) {
        LOG.error("Error executing query", e);
        return "{\"error\": \"" + e.getMessage() + "\"}";
      }
    });
  }
}
