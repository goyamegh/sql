/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;

import java.io.IOException;

public class GetDirectQueryResourcesActionRequest extends ActionRequest {
  private final GetDirectQueryResourcesRequest directQueryRequest;

  public GetDirectQueryResourcesActionRequest(GetDirectQueryResourcesRequest directQueryRequest) {
    this.directQueryRequest = directQueryRequest;
  }

  public GetDirectQueryResourcesActionRequest(StreamInput in) throws IOException {
    super(in);
    // In a real implementation, deserialize the request
    // This is just a placeholder since we don't have the full serialization code
    this.directQueryRequest = new GetDirectQueryResourcesRequest();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    // Add serialization logic if needed
  }

  public GetDirectQueryResourcesRequest getDirectQueryRequest() {
    return directQueryRequest;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
