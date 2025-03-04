/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.query;

import java.util.List;
import java.util.Optional;

import org.opensearch.common.inject.Inject;

/**
 * Registry for all query handlers.
 */
public class QueryHandlerRegistry {
  
  private final List<QueryHandler> handlers;
  
  @Inject
  public QueryHandlerRegistry(List<QueryHandler> handlers) {
    this.handlers = handlers;
  }
  
  /**
   * Finds a handler that can process the given client.
   * 
   * @param client The client to find a handler for
   * @return An optional containing the handler if found
   */
  public Optional<QueryHandler> findHandler(Object client) {
    return handlers.stream()
        .filter(handler -> handler.canHandle(client))
        .findFirst();
  }
}