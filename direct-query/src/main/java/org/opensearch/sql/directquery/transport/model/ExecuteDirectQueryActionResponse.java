/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

@RequiredArgsConstructor
public class ExecuteDirectQueryActionResponse extends ActionResponse {

  @Getter private final String queryId;
  @Getter private final String result;
  @Getter private final String sessionId;

  public ExecuteDirectQueryActionResponse(StreamInput in) throws IOException {
    super(in);
    queryId = in.readString();
    result = in.readString();
    sessionId = in.readOptionalString();
  }

  @Override
  public void writeTo(StreamOutput streamOutput) throws IOException {
    streamOutput.writeString(queryId);
    streamOutput.writeString(result);
    streamOutput.writeOptionalString(sessionId);
  }
}
