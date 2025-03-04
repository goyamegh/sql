/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.directquery;

import java.io.IOException;
import java.util.UUID;
import lombok.AllArgsConstructor;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.client.DataSourceClientFactory;
import org.opensearch.sql.datasource.query.QueryHandlerRegistry;
import org.opensearch.sql.spark.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.spark.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.transport.TransportService;

public class DirectQueryExecutorServiceImpl implements DirectQueryExecutorService {

  private DataSourceClientFactory clientFactory;
  private QueryHandlerRegistry queryHandlerRegistry;

  @Inject
  public DirectQueryExecutorServiceImpl(DataSourceClientFactory clientFactory,
                                        QueryHandlerRegistry queryHandlerRegistry) {
    this.clientFactory = clientFactory;
    this.queryHandlerRegistry = queryHandlerRegistry;
  }

  @Override
  public ExecuteDirectQueryResponse executeDirectQuery(ExecuteDirectQueryRequest request) {
    String result;

    // TODO: Replace with the data source query id.
    String queryId = UUID.randomUUID().toString(); // Generate a unique query ID
    String sessionId = request.getSessionId(); // Session ID is passed as is
    try {
      String dataSourceName = request.getDatasource();
      if (dataSourceName == null) {
        return new ExecuteDirectQueryResponse(queryId, "{\"error\": \"Data source name cannot be null\"}", sessionId);
      }
      
      String queryType = request.getQueryType();
      if (queryType == null) {
        return new ExecuteDirectQueryResponse(queryId, "{\"error\": \"Query type cannot be null\"}", sessionId);
      }
      
      // Get the appropriate client for the data source
      Object client = clientFactory.createClient(dataSourceName);
      
      // Find a handler for this client type
      result = queryHandlerRegistry.findHandler(client)
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
}
