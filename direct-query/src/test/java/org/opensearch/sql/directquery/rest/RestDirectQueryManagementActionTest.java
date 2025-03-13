/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest;

import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

public class RestDirectQueryManagementActionTest {

  private OpenSearchSettings settings;
  private NodeClient nodeClient;
  private RestRequest request;
  private RestChannel channel;
  private ThreadPool threadPool;
  private RestDirectQueryManagementAction action;
  private Map<String, String> params;

  @BeforeEach
  public void setup() {
    settings = Mockito.mock(OpenSearchSettings.class);
    channel = Mockito.mock(RestChannel.class);
    nodeClient = Mockito.mock(NodeClient.class);
    threadPool = Mockito.mock(ThreadPool.class);

    mockRequest("/my/path?dataSources=prometheus");

    action = new RestDirectQueryManagementAction(settings);
  }

  private void mockRequest(String path) {
    // Set up your required mocks for HttpRequest and HttpChannel
    NamedXContentRegistry xContentRegistry = NamedXContentRegistry.EMPTY;
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    HttpChannel httpChannel = Mockito.mock(HttpChannel.class);

    // Stub basic behavior: uri and headers
    Mockito.when(httpRequest.uri()).thenReturn(path);
    Mockito.when(httpRequest.getHeaders()).thenReturn(new HashMap<>());
    // Stub HTTP method
    Mockito.when(httpRequest.method()).thenReturn(Method.POST);
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    Mockito.when(httpRequest.content()).thenReturn(new BytesArray(requestContent));

    // Create a real instance via the factory method
    RestRequest realRequest = RestRequest.request(xContentRegistry, httpRequest, httpChannel);

    // Create a spy so that real methods (including param()) are invoked
    request = Mockito.spy(realRequest);
  }

  @Test
  @SneakyThrows
  public void testGetName() {
    Assertions.assertEquals(RestDirectQueryManagementAction.DIRECT_QUERY_ACTIONS, action.getName());
  }

  @Test
  @SneakyThrows
  public void testRoutes() {
    List<Route> routes = action.routes();
    Assertions.assertNotNull(routes);
    Assertions.assertFalse(routes.isEmpty());

    // Verify the route matches what you expect
    boolean foundExpectedRoute = false;
    for (Route route : routes) {
      if (Method.POST.equals(route.getMethod())
          && RestDirectQueryManagementAction.BASE_DIRECT_QUERY_ACTION_URL.equals(route.getPath())) {
        foundExpectedRoute = true;
        break;
      }
    }
    Assertions.assertTrue(foundExpectedRoute, "Expected route not found");
  }

  @Test
  public void testPrepareRequestWhenDataSourcesNotEnabled() throws Exception {
    // Set up the mock to return false for DATASOURCES_ENABLED
    when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(false);

    // Add this line to ensure POST method
    when(request.method()).thenReturn(Method.POST);

    // Use the handleRequest method
    action.handleRequest(request, channel, nodeClient);

    // Verify that the client was not interacted with
    Mockito.verifyNoInteractions(threadPool);

    // Capture and verify the response
    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(responseCaptor.capture());
    Assertions.assertEquals(400, responseCaptor.getValue().status().getStatus());
  }

  @Test
  public void testExecuteDirectQueryRequest() throws Exception {
    // Set up datasources enabled
    when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(true);

    // Ensure POST method
    when(request.method()).thenReturn(Method.POST);

    // Execute the request
    action.handleRequest(request, channel, nodeClient);

    // Capture and verify the response - the action is reporting an error
    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(responseCaptor.capture());

    // You may want to check the specific error status/message depending on what you expect
    // For example:
    Assertions.assertEquals(400, responseCaptor.getValue().status().getStatus());
  }
}
