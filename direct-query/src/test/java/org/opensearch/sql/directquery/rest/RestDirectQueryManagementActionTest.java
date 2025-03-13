package org.opensearch.sql.directquery.rest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockSettings;
import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class RestDirectQueryManagementActionTest {

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

    unit = new RestDirectQueryManagementAction(settings);
  }

  @Test
  @SneakyThrows
  public void testWhenDataSourcesAreDisabled() {
    setDataSourcesEnabled(false);
    unit.handleRequest(request, channel, nodeClient);
    Mockito.verifyNoInteractions(nodeClient);
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
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    Mockito.when(request.contentParser()).thenReturn(
        new org.opensearch.common.xcontent.json.JsonXContentParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));

    unit.handleRequest(request, channel, nodeClient);
    Mockito.verify(threadPool, Mockito.times(1))
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verifyNoInteractions(channel);
  }

  @Test
  @SneakyThrows
  public void testUnsupportedMethod() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(request.consumedParams()).thenReturn(new java.util.ArrayList<>());
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
  public void testRoutes() {
    List<RestDirectQueryManagementAction.Route> routes = unit.routes();
    Assertions.assertNotNull(routes);
    Assertions.assertEquals(1, routes.size());

    RestDirectQueryManagementAction.Route route = routes.get(0);
    Assertions.assertEquals(RestRequest.Method.POST, route.getMethod());
    Assertions.assertEquals("/_plugins/_directquery/_query/{dataSources}", route.getPath());
  }

  @Test
  @SneakyThrows
  public void testSuccessfulResponse() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    Mockito.when(request.contentParser()).thenReturn(
        new org.opensearch.common.xcontent.json.JsonXContentParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));
    Mockito.when(request.consumedParams()).thenReturn(java.util.Collections.emptyList());
    Mockito.when(request.params()).thenReturn(java.util.Collections.emptyMap());

    ArgumentCaptor<org.opensearch.core.action.ActionListener> listenerCaptor =
        ArgumentCaptor.forClass(org.opensearch.core.action.ActionListener.class);

    Mockito.doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(threadPool).schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(invocation -> {
      ActionListener listener = invocation.getArgument(2);
      return null;
    }).when(nodeClient).execute(
        Mockito.any(),
        Mockito.any(),
        listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);

    String successResponse = "{\"schema\":[{\"name\":\"id\",\"type\":\"integer\"}],\"datarows\":[[1],[2]]}";
    ExecuteDirectQueryActionResponse response = Mockito.mock(ExecuteDirectQueryActionResponse.class);
    Mockito.when(response.getResult()).thenReturn(successResponse);

    ActionListener listener = listenerCaptor.getValue();
    listener.onResponse(response);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(200, capturedResponse.status().getStatus());
    Assertions.assertEquals("application/json; charset=UTF-8", capturedResponse.contentType());
    Assertions.assertEquals(successResponse, capturedResponse.content().utf8ToString());
  }

  @Test
  @SneakyThrows
  public void testBadRequestResponse() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    Mockito.when(request.contentParser()).thenReturn(
        new org.opensearch.common.xcontent.json.JsonXContentParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));
    Mockito.when(request.consumedParams()).thenReturn(java.util.Collections.emptyList());
    Mockito.when(request.params()).thenReturn(java.util.Collections.emptyMap());

    ArgumentCaptor<org.opensearch.core.action.ActionListener> listenerCaptor =
        ArgumentCaptor.forClass(org.opensearch.core.action.ActionListener.class);

    Mockito.doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(threadPool).schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(invocation -> {
      ActionListener listener = invocation.getArgument(2);
      return null;
    }).when(nodeClient).execute(
        Mockito.any(),
        Mockito.any(),
        listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);

    IllegalArgumentException clientError = new IllegalArgumentException("Invalid request parameter");

    ActionListener listener = listenerCaptor.getValue();
    listener.onFailure(clientError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(400, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(400, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals("IllegalArgumentException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals("Invalid Request",
        actualResponseJson.getAsJsonObject("error").get("reason").getAsString());
  }

  @Test
  @SneakyThrows
  public void testInternalServerErrorResponse() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    Mockito.when(request.contentParser()).thenReturn(
        new org.opensearch.common.xcontent.json.JsonXContentParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));
    Mockito.when(request.consumedParams()).thenReturn(java.util.Collections.emptyList());
    Mockito.when(request.params()).thenReturn(java.util.Collections.emptyMap());

    ArgumentCaptor<org.opensearch.core.action.ActionListener> listenerCaptor =
        ArgumentCaptor.forClass(org.opensearch.core.action.ActionListener.class);

    Mockito.doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(threadPool).schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(invocation -> {
      ActionListener listener = invocation.getArgument(2);
      return null;
    }).when(nodeClient).execute(
        Mockito.any(),
        Mockito.any(),
        listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);

    RuntimeException serverError = new RuntimeException("Internal server error");

    ActionListener listener = listenerCaptor.getValue();
    listener.onFailure(serverError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(500, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(500, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals("RuntimeException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals("There was internal problem at backend",
        actualResponseJson.getAsJsonObject("error").get("reason").getAsString());
  }

  @Test
  @SneakyThrows
  public void testOpenSearchException() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    Mockito.when(request.contentParser()).thenReturn(
        new org.opensearch.common.xcontent.json.JsonXContentParser(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
            org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));
    Mockito.when(request.consumedParams()).thenReturn(java.util.Collections.emptyList());
    Mockito.when(request.params()).thenReturn(java.util.Collections.emptyMap());

    ArgumentCaptor<org.opensearch.core.action.ActionListener> listenerCaptor =
        ArgumentCaptor.forClass(org.opensearch.core.action.ActionListener.class);

    Mockito.doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(threadPool).schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(invocation -> {
      ActionListener listener = invocation.getArgument(2);
      return null;
    }).when(nodeClient).execute(
        Mockito.any(),
        Mockito.any(),
        listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);

    OpenSearchException opensearchError = new OpenSearchException("OpenSearch specific error");

    ActionListener listener = listenerCaptor.getValue();
    listener.onFailure(opensearchError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(500, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(500, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals("OpenSearchException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals("OpenSearch specific error",
        actualResponseJson.getAsJsonObject("error").get("details").getAsString());
  }

  private void setDataSourcesEnabled(boolean value) {
    Mockito.when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(value);
  }
}
