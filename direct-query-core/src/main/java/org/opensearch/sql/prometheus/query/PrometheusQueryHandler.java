/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.directquery.rest.model.BaseDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.prometheus.request.system.model.MetricMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PrometheusQueryHandler implements QueryHandler<PrometheusClient> {
  private static final Logger LOG = LogManager.getLogger(PrometheusQueryHandler.class);

  @Override
  public DataSourceType getSupportedDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }

  @Override
  public boolean canHandle(Object client) {
    return client instanceof PrometheusClient;
  }

  @Override
  public Class<PrometheusClient> getClientClass() {
    return PrometheusClient.class;
  }

  @Override
  public String executeQuery(PrometheusClient client, BaseDirectQueryRequest request) throws IOException {
    if (request instanceof GetDirectQueryResourcesRequest) {
      return this.executeGetResourcesQuery(client, (GetDirectQueryResourcesRequest) request);
    }
    return SecurityAccess.doPrivileged(() -> {
      try {
        PrometheusOptions options = ((ExecuteDirectQueryRequest) request).getPrometheusOptions();
        String startTimeStr = options.getStart();
        String endTimeStr = options.getEnd();

        if (startTimeStr == null || endTimeStr == null) {
          return "{\"error\": \"Start and end times are required for Prometheus queries\"}";
        }

        if (options.getQueryType() == PrometheusQueryType.RANGE) {

          JSONObject metricData = client.queryRange(
              ((ExecuteDirectQueryRequest) request).getQuery(),
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

  private String executeGetResourcesQuery(PrometheusClient client, GetDirectQueryResourcesRequest request) {
    return SecurityAccess.doPrivileged(() -> {
      try {
        switch (request.getResourceType().toUpperCase()) {
          case "LABELS":
            List<String> labels = client.getLabels(request.getQueryParams());
            return new JSONArray(labels).toString();
          case "LABEL":
            List<String> labelValues = client.getLabel(request.getResourceName(), request.getQueryParams());
            return new JSONArray(labelValues).toString();
          case "METADATA":
            Map<String, List<MetricMetadata>> metadata = client.getAllMetrics(request.getQueryParams());
            return new JSONObject(metadata).toString();
          case "SERIES":
            List<Map<String, String>> series = client.getSeries(request.getQueryParams());
            return new JSONArray(series).toString();
        }
        return "{\"error\": \"Invalid resource type: " + request.getResourceType() + "\"}";
      } catch (IOException e) {
        LOG.error("Error executing query", e);
        return "{\"error\": \"" + e.getMessage() + "\"}";
      }
    });
  }
}
