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
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class RestDirectQueryResourcesManagementActionTest {

  private OpenSearchSettings settings;
  private RestRequest request;
  private RestChannel channel;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private RestDirectQueryResourcesManagementAction unit;

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

    unit = new RestDirectQueryResourcesManagementAction(settings);
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
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);

    unit.handleRequest(request, channel, nodeClient);
    // Verify that nodeClient.execute was called, which happens inside the Scheduler
    Mockito.verify(threadPool, Mockito.times(1))
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verifyNoInteractions(channel);
  }

  @Test
  @SneakyThrows
  public void testUnsupportedMethod() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.PUT);
    // Mock consumedParams to return a modifiable empty list
    Mockito.when(request.consumedParams()).thenReturn(new java.util.ArrayList<>());
    unit.handleRequest(request, channel, nodeClient);

    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(405, response.getValue().status().getStatus());
  }

  @Test
  public void testGetName() {
    Assertions.assertEquals("direct_query_resources_actions", unit.getName());
  }

  @Test
  @SneakyThrows
  public void testSuccessfulResponse() {
    // Setup
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(request.param("dataSource")).thenReturn("testDataSource");
    Mockito.when(request.param("resourceType")).thenReturn("testResourceType");
    Mockito.when(request.consumedParams()).thenReturn(java.util.Collections.emptyList());
    Mockito.when(request.params()).thenReturn(java.util.Collections.emptyMap());

    // Capture the ActionListener passed to execute()
    ArgumentCaptor<org.opensearch.core.action.ActionListener> listenerCaptor =
        ArgumentCaptor.forClass(org.opensearch.core.action.ActionListener.class);

    // Mock the threadPool.schedule to immediately execute the Runnable
    Mockito.doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(threadPool).schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    // Mock the nodeClient.execute to capture the listener
    Mockito.doAnswer(invocation -> {
      ActionListener listener = invocation.getArgument(2);
      return null;
    }).when(nodeClient).execute(
        Mockito.any(),
        Mockito.any(),
        listenerCaptor.capture());

    // Execute the request
    unit.handleRequest(request, channel, nodeClient);

    // Create a mock response
    String successResponse = "{\"result\":\"success\"}";
    org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionResponse response =
        new org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionResponse(successResponse);

    // Trigger the onResponse method with the captured listener
    ActionListener listener = listenerCaptor.getValue();
    listener.onResponse(response);

    // Verify the response
    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(200, capturedResponse.status().getStatus());
    Assertions.assertEquals("application/json; charset=UTF-8", capturedResponse.contentType());
    Assertions.assertEquals(successResponse, capturedResponse.content().utf8ToString());
  }

  private void setDataSourcesEnabled(boolean value) {
    Mockito.when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(value);
  }
}
