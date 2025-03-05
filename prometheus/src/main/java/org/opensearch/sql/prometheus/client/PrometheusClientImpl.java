/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.sql.prometheus.exceptions.PrometheusClientException;
import org.opensearch.sql.prometheus.request.system.model.MetricMetadata;

public class PrometheusClientImpl implements PrometheusClient {

  private static final Logger logger = LogManager.getLogger(PrometheusClientImpl.class);

  private final OkHttpClient okHttpClient;

  private final URI uri;

  public PrometheusClientImpl(OkHttpClient okHttpClient, URI uri) {
    this.okHttpClient = okHttpClient;
    this.uri = uri;
  }

  @Override
  public JSONObject queryRange(String query, Long start, Long end, String step) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
            uri.toString().replaceAll("/$", ""),
            URLEncoder.encode(query, StandardCharsets.UTF_8),
            start,
            end,
            step);
    logger.info("Making Prometheus query_range request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    
    logger.info("Executing Prometheus request with headers: {}", request.headers());
    Response response = this.okHttpClient.newCall(request).execute();
    
    logger.info("Received Prometheus response for query_range: code={}", response.code());
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONObject("data");
  }

  @Override
  public List<String> getLabels(String metricName) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/labels?%s=%s",
            uri.toString().replaceAll("/$", ""),
            URLEncoder.encode("match[]", StandardCharsets.UTF_8),
            URLEncoder.encode(metricName, StandardCharsets.UTF_8));
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return toListOfLabels(jsonObject.getJSONArray("data"));
  }

  @Override
  public Map<String, List<MetricMetadata>> getAllMetrics() throws IOException {
    String queryUrl = String.format("%s/api/v1/metadata", uri.toString().replaceAll("/$", ""));
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    TypeReference<HashMap<String, List<MetricMetadata>>> typeRef = new TypeReference<>() {};
    return new ObjectMapper().readValue(jsonObject.getJSONObject("data").toString(), typeRef);
  }

  @Override
  public JSONArray queryExemplars(String query, Long start, Long end) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/query_exemplars?query=%s&start=%s&end=%s",
            uri.toString().replaceAll("/$", ""),
            URLEncoder.encode(query, StandardCharsets.UTF_8),
            start,
            end);
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONArray("data");
  }

  private List<String> toListOfLabels(JSONArray array) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      // __name__ is internal label in prometheus representing the metric name.
      // Exempting this from labels list as it is not required in any of the operations.
      if (!"__name__".equals(array.optString(i))) {
        result.add(array.optString(i));
      }
    }
    return result;
  }

  private JSONObject readResponse(Response response) throws IOException {
    // Log the response information
    logger.debug("Prometheus response code: {}", response.code());
    
    if (response.isSuccessful()) {
        JSONObject jsonObject;
        try {
            String bodyString = Objects.requireNonNull(response.body()).string();
            logger.debug("Prometheus response body: {}", bodyString);
            jsonObject = new JSONObject(bodyString);
        } catch (JSONException jsonException) {
            logger.error("Failed to parse Prometheus response as JSON", jsonException);
            throw new PrometheusClientException(
                "Prometheus returned unexpected body, "
                    + "please verify your prometheus server setup.");
        }
        
        String status = jsonObject.getString("status");
        logger.debug("Prometheus response status: {}", status);
        
        if ("success".equals(status)) {
            return jsonObject;
        } else {
            String errorMessage = jsonObject.getString("error");
            logger.error("Prometheus returned error status: {}", errorMessage);
            throw new PrometheusClientException(errorMessage);
        }
    } else {
        String errorBody = response.body() != null ? response.body().string() : "No response body";
        logger.error("Prometheus request failed with code: {}, error body: {}", response.code(), errorBody);
        throw new PrometheusClientException(
            String.format("Request to Prometheus is Unsuccessful with code: %s. Error details: %s", 
                         response.code(), errorBody));
    }
  }
}
