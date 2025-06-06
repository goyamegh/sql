/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.RelevanceFunctionIT;

public class CalciteRelevanceFunctionIT extends RelevanceFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    // TODO: "https://github.com/opensearch-project/sql/issues/3462"
    // disallowCalciteFallback();
  }
}
