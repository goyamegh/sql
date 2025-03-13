package org.opensearch.sql.directquery.rest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockSettings;
import org.mockito.Mockito;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.transport.format.DirectQueryRequestConverter;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class RestDirectQueryManagementActionTest2 {

  private OpenSearchSettings settings;
  private RestRequest request;
  private RestChannel channel;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private RestDirectQueryManagementAction unit;

  @BeforeEach
  public void setup() {
    // allow mocking final methods
    MockSettings mockSettings = withSettings().mockMaker("mock-maker-inline");
    settings = mock(OpenSearchSettings.class);
    request = mock(RestRequest.class, mockSettings);
    channel = mock(RestChannel.class);
    nodeClient = mock(NodeClient.class, mockSettings);
    threadPool = mock(ThreadPool.class);

    Mockito.when(nodeClient.threadPool()).thenReturn(threadPool);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    Mockito.when(request.consumedParams()).thenReturn(new ArrayList<>());

    unit = new RestDirectQueryManagementAction(settings);
  }

  @Test
  @SneakyThrows
  public void testWhenDataSourcesAreDisabled() {
    setDataSourcesEnabled(false);
    unit.handleRequest(request, channel, nodeClient);
    Mockito.verifyNoInteractions(threadPool);
    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(400, response.getValue().status().getStatus());
    JsonObject actualResponseJson =
        new Gson().fromJson(response.getValue().content().utf8ToString(), JsonObject.class);
    JsonObject expectedResponseJson = new JsonObject();
    expectedResponseJson.addProperty("status", 400);
    expectedResponseJson.add("error", new JsonObject());
    expectedResponseJson.getAsJsonObject("error").addProperty("type", "IllegalAccessException");
    expectedResponseJson.getAsJsonObject("error").addProperty("reason", "Invalid Request");
    expectedResponseJson
        .getAsJsonObject("error")
        .addProperty("details", "plugins.query.datasources.enabled setting is false");
    Assertions.assertEquals(expectedResponseJson, actualResponseJson);
  }

  @Test
  @SneakyThrows
  public void testWhenDataSourcesAreEnabled() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.contentParser()).thenReturn(null);

    unit.handleRequest(request, channel, nodeClient);
    // We expect an exception because we mocked the contentParser to return null
    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(400, response.getValue().status().getStatus());
  }

  @Test
  @SneakyThrows
  public void testUnsupportedMethod() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    unit.handleRequest(request, channel, nodeClient);

    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(405, response.getValue().status().getStatus());
  }

  @Test
  public void testGetName() {
    Assertions.assertEquals("direct_query_actions", unit.getName());
  }

  @Test
  @SneakyThrows
  public void testDirectQueryRequestConverterBasic() {
    String jsonContent = "{"
        + "\"datasource\": \"prometheus\","
        + "\"query\": \"up\","
        + "\"sessionId\": \"test-session\","
        + "\"language\": \"promql\""
        + "}";

    XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonContent);
    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    Assertions.assertEquals("prometheus", request.getDataSources());
    Assertions.assertEquals("up", request.getQuery());
    Assertions.assertEquals("test-session", request.getSessionId());
    Assertions.assertEquals(LangType.PROMQL, request.getLanguage());
  }

  @Test
  @SneakyThrows
  public void testDirectQueryRequestConverterWithPrometheusOptions() {
    String jsonContent = "{"
        + "\"datasource\": \"prometheus\","
        + "\"query\": \"up\","
        + "\"language\": \"promql\","
        + "\"options\": {"
        + "  \"queryType\": \"range\","
        + "  \"step\": \"15s\","
        + "  \"start\": \"2023-01-01T00:00:00Z\","
        + "  \"end\": \"2023-01-01T01:00:00Z\""
        + "}"
        + "}";

    XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonContent);
    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    Assertions.assertEquals("prometheus", request.getDataSources());
    Assertions.assertEquals("up", request.getQuery());
    Assertions.assertEquals(LangType.PROMQL, request.getLanguage());

    PrometheusOptions options = (PrometheusOptions) request.getOptions();
    Assertions.assertNotNull(options);
    Assertions.assertEquals(PrometheusQueryType.RANGE, options.getQueryType());
    Assertions.assertEquals("15s", options.getStep());
    Assertions.assertEquals("2023-01-01T00:00:00Z", options.getStart());
    Assertions.assertEquals("2023-01-01T01:00:00Z", options.getEnd());
  }

  @Test
  @SneakyThrows
  public void testDirectQueryRequestConverterWithInstantQuery() {
    String jsonContent = "{"
        + "\"datasource\": \"prometheus\","
        + "\"query\": \"up\","
        + "\"language\": \"promql\","
        + "\"options\": {"
        + "  \"queryType\": \"instant\","
        + "  \"time\": \"2023-01-01T00:30:00Z\""
        + "}"
        + "}";

    XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonContent);
    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    Assertions.assertEquals("prometheus", request.getDataSources());
    Assertions.assertEquals(LangType.PROMQL, request.getLanguage());

    PrometheusOptions options = (PrometheusOptions) request.getOptions();
    Assertions.assertNotNull(options);
    Assertions.assertEquals(PrometheusQueryType.INSTANT, options.getQueryType());
    Assertions.assertEquals("2023-01-01T00:30:00Z", options.getTime());
  }

  @Test
  @SneakyThrows
  public void testDirectQueryRequestConverterWithMaxResultsAndTimeout() {
    String jsonContent = "{"
        + "\"datasource\": \"prometheus\","
        + "\"query\": \"up\","
        + "\"maxResults\": 1000,"
        + "\"timeout\": 30000"
        + "}";

    XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonContent);
    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    Assertions.assertEquals("prometheus", request.getDataSources());
    Assertions.assertEquals("up", request.getQuery());
    Assertions.assertEquals(1000, request.getMaxResults());
    Assertions.assertEquals(30000, request.getTimeout());
  }

  @Test
  @SneakyThrows
  public void testDirectQueryRequestConverterWithInvalidLanguage() {
    String jsonContent = "{"
        + "\"datasource\": \"prometheus\","
        + "\"query\": \"up\","
        + "\"language\": \"invalid_language\""
        + "}";

    XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonContent);

    Exception exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DirectQueryRequestConverter.fromXContentParser(parser)
    );

    Assertions.assertTrue(exception.getMessage().contains("Invalid languageType"));
  }

  @Test
  @SneakyThrows
  public void testDirectQueryRequestConverterWithSourceVersion() {
    String jsonContent = "{"
        + "\"datasource\": \"prometheus\","
        + "\"query\": \"up\","
        + "\"sourceVersion\": \"v2.40.0\""
        + "}";

    XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonContent);
    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    Assertions.assertEquals("prometheus", request.getDataSources());
    Assertions.assertEquals("up", request.getQuery());
    Assertions.assertEquals("v2.40.0", request.getSourceVersion());
  }

  private void setDataSourcesEnabled(boolean value) {
    Mockito.when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(value);
  }
}
