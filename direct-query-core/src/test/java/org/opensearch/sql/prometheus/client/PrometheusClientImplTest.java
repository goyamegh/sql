/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.prometheus.exception.PrometheusClientException;

public class PrometheusClientImplTest {

  private MockWebServer mockWebServer;
  private PrometheusClientImpl client;

  @BeforeEach
  public void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();
    OkHttpClient httpClient = new OkHttpClient.Builder().build();
    client =
        new PrometheusClientImpl(
            httpClient,
            URI.create(String.format("http://%s:%s", "localhost", mockWebServer.getPort())));
  }

  @AfterEach
  public void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  public void testQueryRange() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},\"values\":[[1435781430.781,\"1\"],[1435781445.781,\"1\"],[1435781460.781,\"1\"]]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.queryRange("up", 1435781430L, 1435781460L, "15s");

    // Verify
    assertNotNull(result);
    assertEquals("success", result.getString("status"));
    JSONObject data = result.getJSONObject("data");
    assertEquals("matrix", data.getString("resultType"));
    JSONArray resultArray = data.getJSONArray("result");
    assertEquals(1, resultArray.length());
    JSONObject metric = resultArray.getJSONObject(0).getJSONObject("metric");
    assertEquals("up", metric.getString("__name__"));
  }

  @Test
  public void testQueryRangeWith2xxStatusAndError() {
    // Setup
    String errorResponse = "{\"status\":\"error\",\"error\":\"Error\"}";
    mockWebServer.enqueue(new MockResponse().setBody(errorResponse).setResponseCode(200));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> client.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertEquals("Error", exception.getMessage());
  }

  @Test
  public void testQueryRangeWithNonJsonResponse() {
    // Setup
    String nonJsonResponse = "Not a JSON response";
    mockWebServer.enqueue(new MockResponse().setBody(nonJsonResponse).setResponseCode(200));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> client.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Prometheus returned unexpected body, please verify your prometheus server"
                    + " setup."));
  }

  @Test
  public void testQueryRangeWithNon2xxError() {
    // Setup
    mockWebServer.enqueue(new MockResponse().setResponseCode(400).setBody(""));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> client.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertTrue(
        exception.getMessage().contains("Request to Prometheus is Unsuccessful with code: 400"));
  }

  @Test
  public void testQuery() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},\"value\":[1435781460.781,\"1\"]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.query("up", 1435781460L, 100, 30);

    // Verify
    assertNotNull(result);
    assertEquals("success", result.getString("status"));
    JSONObject data = result.getJSONObject("data");
    assertEquals("vector", data.getString("resultType"));
    JSONArray resultArray = data.getJSONArray("result");
    assertEquals(1, resultArray.length());
    JSONObject metric = resultArray.getJSONObject(0).getJSONObject("metric");
    assertEquals("up", metric.getString("__name__"));
  }

  @Test
  public void testGetLabels() throws IOException {
    // Setup
    String successResponse = "{\"status\":\"success\",\"data\":[\"job\",\"instance\",\"version\"]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    List<String> labels = client.getLabels(new HashMap<>());

    // Verify
    assertNotNull(labels);
    assertEquals(3, labels.size());
    assertTrue(labels.contains("job"));
    assertTrue(labels.contains("instance"));
    assertTrue(labels.contains("version"));
  }

  @Test
  public void testGetLabel() throws IOException {
    // Setup
    String successResponse = "{\"status\":\"success\",\"data\":[\"prometheus\",\"node-exporter\"]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    List<String> labelValues = client.getLabel("job", new HashMap<>());

    // Verify
    assertNotNull(labelValues);
    assertEquals(2, labelValues.size());
    assertTrue(labelValues.contains("prometheus"));
    assertTrue(labelValues.contains("node-exporter"));
  }

  @Test
  public void testQueryExemplars() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":[{\"seriesLabels\":{\"__name__\":\"http_request_duration_seconds_bucket\",\"handler\":\"/api/v1/query_range\",\"le\":\"1\"},\"exemplars\":[{\"labels\":{\"traceID\":\"19a801c37fb022d6\"},\"value\":0.207396059,\"timestamp\":1659284721.762}]}]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONArray result =
        client.queryExemplars("http_request_duration_seconds_bucket", 1659284721L, 1659284722L);

    // Verify
    assertNotNull(result);
    assertEquals(1, result.length());
    JSONObject exemplarData = result.getJSONObject(0);
    assertTrue(exemplarData.has("seriesLabels"));
    assertTrue(exemplarData.has("exemplars"));
  }
}
