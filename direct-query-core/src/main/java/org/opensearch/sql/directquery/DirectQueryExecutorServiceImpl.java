/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery;

import java.io.IOException;
import java.util.UUID;

import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.client.DataSourceClientFactory;
import org.opensearch.sql.datasource.query.QueryHandlerRegistry;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;

public class DirectQueryExecutorServiceImpl implements DirectQueryExecutorService {

  private final DataSourceClientFactory dataSourceClientFactory;
  private final QueryHandlerRegistry queryHandlerRegistry;

  @Inject
  public DirectQueryExecutorServiceImpl(DataSourceClientFactory dataSourceClientFactory,
                                        QueryHandlerRegistry queryHandlerRegistry) {
    this.dataSourceClientFactory = dataSourceClientFactory;
    this.queryHandlerRegistry = queryHandlerRegistry;
  }

  @Override
  public ExecuteDirectQueryResponse executeDirectQuery(ExecuteDirectQueryRequest request) {
    // TODO: Replace with the data source query id.
    String queryId = UUID.randomUUID().toString();
    String sessionId = request.getSessionId(); // Session ID is passed as is

    String result;
    try {
      Object client = dataSourceClientFactory.createClient(request.getDataSources());
      result = queryHandlerRegistry.getQueryHandler(client)
          .map(handler -> {
            try {
              return handler.executeQuery(client, request);
            } catch (IOException e) {
              return "{\"error\": \"Error executing query: " + e.getMessage() + "\"}";
            }
          })
          .orElse("{\"error\": \"Unsupported data source type\"}");

    } catch (Exception e) {
      result = "{\"error\": \"" + e.getMessage() + "\"}";
    }
    return new ExecuteDirectQueryResponse(queryId, result, sessionId);
  }

  @Override
  public GetDirectQueryResourcesResponse<?> getDirectQueryResources(
      GetDirectQueryResourcesRequest request) {
    Object client = dataSourceClientFactory.createClient(request.getDataSources());
    return queryHandlerRegistry.getQueryHandler(client)
        .map(handler -> {
          try {
            return handler.getResources(client, request);
          } catch (IOException e) {
            // TODO throw client exception
            return GetDirectQueryResourcesResponse.withError(
                "Error getting resources: " + e.getMessage());
          }
        })
        .orElseThrow(() -> new IllegalArgumentException("Unsupported data source type: " + request.getDataSources()));
  }
}
