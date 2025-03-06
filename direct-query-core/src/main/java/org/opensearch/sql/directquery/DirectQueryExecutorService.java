/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery;

import org.opensearch.sql.directquery.rest.model.BaseDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;

public interface DirectQueryExecutorService {

  /**
   * Execute a direct query request.
   *
   * @param request The direct query request
   * @return A response containing the result
   */
  ExecuteDirectQueryResponse executeDirectQuery(BaseDirectQueryRequest request);
  
}
