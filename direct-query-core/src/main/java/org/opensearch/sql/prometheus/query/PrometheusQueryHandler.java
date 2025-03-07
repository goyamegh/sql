/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.exception.PrometheusClientException;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.prometheus.model.MetricMetadata;

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
  public boolean canHandle(PrometheusClient client) {
    return client instanceof PrometheusClient;
  }

  @Override
  public Class<PrometheusClient> getClientClass() {
    return PrometheusClient.class;
  }

  @Override
  public String executeQuery(PrometheusClient client, ExecuteDirectQueryRequest request) throws IOException {
    return SecurityAccess.doPrivileged(() -> {
      try {
        ExecuteDirectQueryRequest queryRequest = (ExecuteDirectQueryRequest) request;
        PrometheusOptions options = queryRequest.getPrometheusOptions();
        String startTimeStr = options.getStart();
        String endTimeStr = options.getEnd();
        Integer limit = queryRequest.getMaxResults();
        Integer timeout = queryRequest.getTimeout();

        if (options.getQueryType() == PrometheusQueryType.RANGE && (startTimeStr == null || endTimeStr == null)) {
          return "{\"error\": \"Start and end times are required for Prometheus queries\"}";
        } else if (options.getQueryType() == PrometheusQueryType.INSTANT && options.getTime() == null) {
          return "{\"error\": \"Time is required for instant Prometheus queries\"}";
        }

        if (options.getQueryType() == PrometheusQueryType.RANGE) {
          JSONObject metricData = client.queryRange(
              queryRequest.getQuery(),
              Long.parseLong(startTimeStr),
              Long.parseLong(endTimeStr),
              options.getStep(),
              limit,
              timeout);
          return metricData.toString();
        } else if (options.getQueryType() == PrometheusQueryType.INSTANT) {
          JSONObject metricData = client.query(
              queryRequest.getQuery(),
              Long.parseLong(options.getTime()),
              limit,
              timeout);
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

  @Override
  public GetDirectQueryResourcesResponse<?> getResources(PrometheusClient client, GetDirectQueryResourcesRequest request) {
    return SecurityAccess.doPrivileged(() -> {
      try {
        switch (request.getResourceType().toUpperCase()) {
          case "LABELS":
            List<String> labels = client.getLabels(request.getQueryParams());
            return GetDirectQueryResourcesResponse.withStringList(labels);
          case "LABEL":
            List<String> labelValues = client.getLabel(request.getResourceName(), request.getQueryParams());
            return GetDirectQueryResourcesResponse.withStringList(labelValues);
          case "METADATA":
            Map<String, List<MetricMetadata>> metadata = client.getAllMetrics(request.getQueryParams());
            return GetDirectQueryResourcesResponse.withMap(metadata);
          case "SERIES":
            List<Map<String, String>> series = client.getSeries(request.getQueryParams());
            return GetDirectQueryResourcesResponse.withStringMapList(series);
          default:
            throw new IllegalArgumentException("Invalid resource type: " + request.getResourceType());
        }
      } catch (IOException e) {
        LOG.error("Error getting resources", e);
        throw new PrometheusClientException(String.format("Error while getting resources for %s: %s", request.getResourceType(), e.getMessage()));
      }
    });
  }
}
