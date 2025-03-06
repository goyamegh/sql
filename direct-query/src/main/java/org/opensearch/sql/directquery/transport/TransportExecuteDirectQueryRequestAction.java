/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.directquery.DirectQueryExecutorService;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.directquery.rest.model.BaseDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionRequest;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportExecuteDirectQueryRequestAction
    extends HandledTransportAction<ExecuteDirectQueryActionRequest, ExecuteDirectQueryActionResponse> {

  private final DirectQueryExecutorService directQueryExecutorService;
  
  public static final String NAME = "indices:data/read/direct_query";
  public static final ActionType<ExecuteDirectQueryActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, ExecuteDirectQueryActionResponse::new);

  @Inject
  public TransportExecuteDirectQueryRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DirectQueryExecutorServiceImpl directQueryExecutorService) {
    super(NAME, transportService, actionFilters, ExecuteDirectQueryActionRequest::new);
    this.directQueryExecutorService = (DirectQueryExecutorService) directQueryExecutorService;
  }

  @Override
  protected void doExecute(
      Task task,
      ExecuteDirectQueryActionRequest request,
      ActionListener<ExecuteDirectQueryActionResponse> listener) {
    try {
      BaseDirectQueryRequest directQueryRequest = request.getDirectQueryRequest();
      
      ExecuteDirectQueryResponse response = directQueryExecutorService.executeDirectQuery(directQueryRequest);
      String responseContent =
          new JsonResponseFormatter<ExecuteDirectQueryResponse>(JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(ExecuteDirectQueryResponse response) {
              return response;
            }
          }.format(response);
      listener.onResponse(new ExecuteDirectQueryActionResponse(
          response.getQueryId(),
          responseContent,
          response.getSessionId()));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
