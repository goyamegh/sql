/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.GET;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.datasources.exceptions.ErrorMessage;
import org.opensearch.sql.datasources.utils.Scheduler;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.util.RestRequestUtil;
import org.opensearch.sql.spark.transport.TransportExecuteDirectQueryRequestAction;
import org.opensearch.sql.spark.transport.format.DirectQueryRequestConverter;
import org.opensearch.sql.spark.transport.model.ExecuteDirectQueryActionRequest;
import org.opensearch.sql.spark.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.transport.client.node.NodeClient;

@RequiredArgsConstructor
public class RestDirectQueryManagementAction extends BaseRestHandler {

  public static final String DIRECT_QUERY_ACTIONS = "direct_query_actions";
  public static final String BASE_DIRECT_QUERY_ACTION_URL =
      "/_plugins/_direct_query/_sync/{dataSources}";

  private static final Logger LOG = LogManager.getLogger(RestDirectQueryManagementAction.class);
  private final OpenSearchSettings settings;

  @Override
  public String getName() {
    return DIRECT_QUERY_ACTIONS;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(
        new Route(GET, "/_plugins/_direct_query/resources/{dataSources}/api/v1/{resourceType}"),
        new Route(POST, BASE_DIRECT_QUERY_ACTION_URL)
    );
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    if (!dataSourcesEnabled()) {
      return dataSourcesDisabledError(restRequest);
    }


    switch (restRequest.method()) {
      case GET:
        return executeGetResourcesRequest(restRequest, nodeClient);
      case POST:
        // Extract the datasource name from the URL path
        String dataSourceFromPath = restRequest.param("dataSources");
        return executeDirectQueryRequest(restRequest, nodeClient, dataSourceFromPath);
      default:
        return restChannel ->
            restChannel.sendResponse(
                new BytesRestResponse(
                    RestStatus.METHOD_NOT_ALLOWED, String.valueOf(restRequest.method())));
    }
  }

  private RestChannelConsumer executeGetResourcesRequest(
      RestRequest restRequest, NodeClient nodeClient) {
    ExecuteDirectQueryRequest directQueryRequest = new ExecuteDirectQueryRequest();
    directQueryRequest.setDatasource(restRequest.param("dataSources"));
    directQueryRequest.setQueryType(restRequest.param("resourceType"));
    directQueryRequest.setQueryParams(restRequest.params().keySet().stream()
        .filter(p -> !restRequest.consumedParams().contains(p))
        .collect(Collectors.toMap(p -> p, restRequest::param)));

    return restChannel -> {
      try {
        // Generate a queryId for tracking and potential cancellation
        String queryId = UUID.randomUUID().toString();
        directQueryRequest.setQueryId(queryId);

        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportExecuteDirectQueryRequestAction.ACTION_TYPE,
                    new ExecuteDirectQueryActionRequest(directQueryRequest),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(ExecuteDirectQueryActionResponse response) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                response.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel, restRequest.method());
                      }
                    }));
      } catch (Exception e) {
        handleException(e, restChannel, restRequest.method());
      }
    };
  }

  private RestChannelConsumer executeDirectQueryRequest(
      RestRequest restRequest, NodeClient nodeClient, String dataSourceFromPath) {
    return restChannel -> {
      try {
        ExecuteDirectQueryRequest directQueryRequest =
            DirectQueryRequestConverter.fromXContentParser(restRequest.contentParser());

        // If the datasource is not specified in the payload, use the path parameter
        if (directQueryRequest.getDatasource() == null) {
          directQueryRequest.setDatasource(dataSourceFromPath);
        }

        // Generate a queryId for tracking and potential cancellation
        String queryId = UUID.randomUUID().toString();
        directQueryRequest.setQueryId(queryId);

        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportExecuteDirectQueryRequestAction.ACTION_TYPE,
                    new ExecuteDirectQueryActionRequest(directQueryRequest),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(ExecuteDirectQueryActionResponse response) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                response.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel, restRequest.method());
                      }
                    }));
      } catch (Exception e) {
        handleException(e, restChannel, restRequest.method());
      }
    };
  }

  private void handleException(Exception e, RestChannel restChannel, RestRequest.Method requestMethod) {
    if (e instanceof OpenSearchException) {
      OpenSearchException exception = (OpenSearchException) e;
      reportError(restChannel, exception, exception.status());
    } else {
      LOG.error("Error happened during request handling", e);
      if (isClientError(e)) {
        reportError(restChannel, e, BAD_REQUEST);
      } else {
        reportError(restChannel, e, INTERNAL_SERVER_ERROR);
      }
    }
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(status, new ErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof IllegalArgumentException
        || e instanceof IllegalStateException
        || e instanceof DataSourceClientException
        || e instanceof IllegalAccessException;
  }

  private boolean dataSourcesEnabled() {
    return settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED);
  }

  private RestChannelConsumer dataSourcesDisabledError(RestRequest request) {
    RestRequestUtil.consumeAllRequestParameters(request);

    return channel -> {
      reportError(
          channel,
          new IllegalAccessException(
              String.format("%s setting is false", Settings.Key.DATASOURCES_ENABLED.getKeyValue())),
          BAD_REQUEST);
    };
  }
}
