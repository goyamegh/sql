/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionRequest;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportExecuteDirectQueryRequestActionTest2 {

  @Mock private TransportService transportService;
  @Mock private TransportExecuteDirectQueryRequestAction action;
  @Mock private Task task;
  @Mock private ActionListener<ExecuteDirectQueryActionResponse> actionListener;
  @Mock private DirectQueryExecutorServiceImpl directQueryExecutorService;

  @Captor
  private ArgumentCaptor<ExecuteDirectQueryActionResponse> executeQueryResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportExecuteDirectQueryRequestAction(
            transportService, new ActionFilters(new HashSet<>()), directQueryExecutorService);
  }

  @Test
  public void testDoExecute() {
    // Prepare test data
    ExecuteDirectQueryRequest directQueryRequest = new ExecuteDirectQueryRequest();
    directQueryRequest.setQuery("SELECT * FROM test");
    ExecuteDirectQueryActionRequest request = new ExecuteDirectQueryActionRequest(directQueryRequest);
    
    ExecuteDirectQueryResponse directQueryResponse = new ExecuteDirectQueryResponse("query-123", null, null);
    when(directQueryExecutorService.executeDirectQuery(any(ExecuteDirectQueryRequest.class)))
        .thenReturn(directQueryResponse);

    // Execute the action
    action.doExecute(task, request, actionListener);

    // Verify the response
    verify(actionListener).onResponse(executeQueryResponseArgumentCaptor.capture());
    ExecuteDirectQueryActionResponse response = executeQueryResponseArgumentCaptor.getValue();
    Assertions.assertEquals("query-123", response.getQueryId());
  }

  @Test
  public void testDoExecuteWithSessionId() {
    // Prepare test data
    ExecuteDirectQueryRequest directQueryRequest = new ExecuteDirectQueryRequest();
    directQueryRequest.setQuery("SELECT * FROM test");
    directQueryRequest.setSessionId("session-456");
    ExecuteDirectQueryActionRequest request = new ExecuteDirectQueryActionRequest(directQueryRequest);
    
    ExecuteDirectQueryResponse directQueryResponse = new ExecuteDirectQueryResponse("query-123", null, "session-456");
    when(directQueryExecutorService.executeDirectQuery(any(ExecuteDirectQueryRequest.class)))
        .thenReturn(directQueryResponse);

    // Execute the action
    action.doExecute(task, request, actionListener);

    // Verify the response
    verify(actionListener).onResponse(executeQueryResponseArgumentCaptor.capture());
    ExecuteDirectQueryActionResponse response = executeQueryResponseArgumentCaptor.getValue();
    Assertions.assertEquals("query-123", response.getQueryId());
    Assertions.assertEquals("session-456", response.getSessionId());
  }

  @Test
  public void testDoExecuteWithException() {
    // Prepare test data
    ExecuteDirectQueryRequest directQueryRequest = new ExecuteDirectQueryRequest();
    directQueryRequest.setQuery("INVALID QUERY");
    ExecuteDirectQueryActionRequest request = new ExecuteDirectQueryActionRequest(directQueryRequest);
    
    RuntimeException exception = new RuntimeException("Invalid SQL query");
    doThrow(exception).when(directQueryExecutorService).executeDirectQuery(any(ExecuteDirectQueryRequest.class));

    // Execute the action
    action.doExecute(task, request, actionListener);

    // Verify the exception handling
    verify(directQueryExecutorService, times(1)).executeDirectQuery(any(ExecuteDirectQueryRequest.class));
    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception capturedException = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(capturedException instanceof RuntimeException);
    Assertions.assertEquals("Invalid SQL query", capturedException.getMessage());
  }
}
