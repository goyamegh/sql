/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import lombok.experimental.UtilityClass;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.rest.model.ExecuteDirectQueryRequest;

@UtilityClass
public class DirectQueryRequestConverter {
  
  public static ExecuteDirectQueryRequest fromXContentParser(XContentParser parser) throws Exception {
    String datasource = null;
    String query = null;
    String queryType = null;
    String startTime = null;
    String endTime = null;
    String step = null;
    String sessionId = null;
    String queryId = null;
    
    try {
      ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case "datasource":
            datasource = parser.textOrNull();
            break;
          case "query":
            query = parser.textOrNull();
            break;
          case "queryType":
            queryType = parser.textOrNull();
            break;
          case "startTime":
            startTime = parser.textOrNull();
            break;
          case "endTime":
            endTime = parser.textOrNull();
            break;
          case "step":
            step = parser.textOrNull();
            break;
          case "sessionId":
            sessionId = parser.textOrNull();
            break;
          case "queryId":
            queryId = parser.textOrNull();
            break;
          default:
            throw new IllegalArgumentException("Unknown field: " + fieldName);
        }
      }
      
      ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
      request.setDatasource(datasource);
      request.setQuery(query);
      request.setQueryType(queryType);
      request.setStartTime(startTime);
      request.setEndTime(endTime);
      request.setStep(step);
      request.setSessionId(sessionId);
      request.setQueryId(queryId);
      
      return request;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Error while parsing the direct query request: %s", e.getMessage()), e);
    }
  }
}
