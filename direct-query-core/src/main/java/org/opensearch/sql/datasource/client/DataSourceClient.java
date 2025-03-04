/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import java.io.IOException;
import java.util.Map;

import org.opensearch.sql.datasource.model.DataSourceType;
import org.json.JSONObject;

/**
 * Common interface for all data source clients.
 */
public interface DataSourceClient {

  /**
   * Returns the type of data source this client supports.
   *
   * @return The data source type
   */
  DataSourceType getDataSourceType();

  /**
   * Execute a query against the data source.
   *
   * @param queryType The type of query to execute
   * @param query The query string
   * @param params Additional parameters for the query
   * @return Query results as JSONObject
   * @throws IOException If query execution fails
   */
  JSONObject executeQuery(String queryType, String query, Map<String, Object> params) throws IOException;
}