/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

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
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashSet;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TransportGetDirectQueryResourcesRequestActionTest {

  @Mock
  private TransportService transportService;
  @Mock
  private TransportGetDirectQueryResourcesRequestAction action;
  @Mock
  private Task task;
  @Mock
  private ActionListener<GetDirectQueryResourcesActionResponse> actionListener;
  @Mock
  private DirectQueryExecutorServiceImpl directQueryExecutorService;

  @Captor
  private ArgumentCaptor<GetDirectQueryResourcesActionResponse> resourcesResponseArgumentCaptor;

  @Captor
  private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportGetDirectQueryResourcesRequestAction(
            transportService, new ActionFilters(new HashSet<>()), directQueryExecutorService);
  }


  @Test
  public void testDoExecute() {
    // Prepare test data
    GetDirectQueryResourcesRequest resourcesRequest = new GetDirectQueryResourcesRequest();
    GetDirectQueryResourcesActionRequest request = new GetDirectQueryResourcesActionRequest(resourcesRequest);

    GetDirectQueryResourcesResponse resourcesResponse = GetDirectQueryResourcesResponse.withStringList(List.of("mock-response-1", "mock-response"));
    when(directQueryExecutorService.getDirectQueryResources(any(GetDirectQueryResourcesRequest.class)))
        .thenReturn(resourcesResponse);

    // Execute the action
    action.doExecute(task, request, actionListener);

    // Verify the response
    verify(actionListener).onResponse(resourcesResponseArgumentCaptor.capture());
    GetDirectQueryResourcesActionResponse response = resourcesResponseArgumentCaptor.getValue();
    Assertions.assertNotNull(response.getResult());
    Assertions.assertFalse(response.getResult().isEmpty());
  }

  @Test
  public void testDoExecuteWithException() {
    // Prepare test data
    GetDirectQueryResourcesRequest resourcesRequest = new GetDirectQueryResourcesRequest();
    GetDirectQueryResourcesActionRequest request = new GetDirectQueryResourcesActionRequest(resourcesRequest);

    RuntimeException exception = new RuntimeException("Session not found");
    doThrow(exception).when(directQueryExecutorService).getDirectQueryResources(any(GetDirectQueryResourcesRequest.class));

    // Execute the action
    action.doExecute(task, request, actionListener);

    // Verify the exception handling
    verify(directQueryExecutorService, times(1)).getDirectQueryResources(any(GetDirectQueryResourcesRequest.class));
    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception capturedException = exceptionArgumentCaptor.getValue();
    Assertions.assertInstanceOf(RuntimeException.class, capturedException);
    Assertions.assertEquals("Session not found", capturedException.getMessage());
  }
}
