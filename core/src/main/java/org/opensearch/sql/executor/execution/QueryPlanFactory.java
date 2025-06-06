/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static java.util.Objects.requireNonNull;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.exception.UnsupportedCursorRequestException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.pagination.CanPaginateVisitor;

/** QueryExecution Factory. */
@RequiredArgsConstructor
public class QueryPlanFactory
    extends AbstractNodeVisitor<
        AbstractPlan,
        Pair<
            ResponseListener<ExecutionEngine.QueryResponse>,
            ResponseListener<ExecutionEngine.ExplainResponse>>> {

  /** Query Service. */
  private final QueryService queryService;

  /**
   * NO_CONSUMER_RESPONSE_LISTENER should never be called. It is only used as constructor parameter
   * of {@link QueryPlan}.
   */
  public static final ResponseListener<ExecutionEngine.QueryResponse>
      NO_CONSUMER_RESPONSE_LISTENER =
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse response) {
              throw new IllegalStateException(
                  "[BUG] query response should not sent to unexpected channel");
            }

            @Override
            public void onFailure(Exception e) {
              throw new IllegalStateException(
                  "[BUG] exception response should not sent to unexpected channel");
            }
          };

  /** Create QueryExecution from Statement. */
  public AbstractPlan create(
      Statement statement,
      ResponseListener<ExecutionEngine.QueryResponse> queryListener,
      ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    return statement.accept(this, Pair.of(queryListener, explainListener));
  }

  /** Creates a QueryPlan from a cursor. */
  public AbstractPlan create(
      String cursor,
      boolean isExplain,
      QueryType queryType,
      String format,
      ResponseListener<ExecutionEngine.QueryResponse> queryResponseListener,
      ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    QueryId queryId = QueryId.queryId();
    var plan =
        new QueryPlan(
            queryId, queryType, new FetchCursor(cursor), queryService, queryResponseListener);
    return isExplain
        ? new ExplainPlan(queryId, queryType, plan, Explain.format(format), explainListener)
        : plan;
  }

  boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
  }

  /** Creates a {@link CloseCursor} command on a cursor. */
  public AbstractPlan createCloseCursor(
      String cursor,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> queryResponseListener) {
    return new CommandPlan(
        QueryId.queryId(),
        queryType,
        new CloseCursor().attach(new FetchCursor(cursor)),
        queryService,
        queryResponseListener);
  }

  @Override
  public AbstractPlan visitQuery(
      Query node,
      Pair<
              ResponseListener<ExecutionEngine.QueryResponse>,
              ResponseListener<ExecutionEngine.ExplainResponse>>
          context) {
    requireNonNull(context.getLeft(), "[BUG] query listener must be not null");
    if (node.getFetchSize() > 0) {
      if (canConvertToCursor(node.getPlan())) {
        return new QueryPlan(
            QueryId.queryId(),
            node.getQueryType(),
            node.getPlan(),
            node.getFetchSize(),
            queryService,
            context.getLeft());
      } else {
        // This should be picked up by the legacy engine.
        throw new UnsupportedCursorRequestException();
      }
    } else {
      return new QueryPlan(
          QueryId.queryId(), node.getQueryType(), node.getPlan(), queryService, context.getLeft());
    }
  }

  @Override
  public AbstractPlan visitExplain(
      Explain node,
      Pair<
              ResponseListener<ExecutionEngine.QueryResponse>,
              ResponseListener<ExecutionEngine.ExplainResponse>>
          context) {
    requireNonNull(context.getRight(), "[BUG] explain listener must be not null");
    return new ExplainPlan(
        QueryId.queryId(),
        node.getQueryType(),
        create(node.getStatement(), NO_CONSUMER_RESPONSE_LISTENER, context.getRight()),
        node.getFormat(),
        context.getRight());
  }
}
