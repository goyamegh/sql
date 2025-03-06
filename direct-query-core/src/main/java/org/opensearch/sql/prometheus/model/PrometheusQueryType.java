/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.model;

/**
 * Enum representing the types of Prometheus queries.
 */
public enum PrometheusQueryType {
  INSTANT("instant"),
  RANGE("range");

  private final String value;

  PrometheusQueryType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static PrometheusQueryType fromString(String text) {
    for (PrometheusQueryType type : PrometheusQueryType.values()) {
      if (type.value.equalsIgnoreCase(text)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown query type: " + text);
  }

  @Override
  public String toString() {
    return value;
  }
}