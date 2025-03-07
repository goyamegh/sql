/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.common.interceptors.AwsSigningInterceptor;
import org.opensearch.sql.common.interceptors.BasicAuthenticationInterceptor;
import org.opensearch.sql.common.interceptors.URIValidatorInterceptor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import lombok.RequiredArgsConstructor;

import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import okhttp3.OkHttpClient;
import org.opensearch.sql.opensearch.security.SecurityAccess;

/**
 * Factory for creating data source clients based on the data source type.
 */
public class DataSourceClientFactory {

  public static final String URI = "prometheus.uri";
  public static final String AUTH_TYPE = "prometheus.auth.type";
  public static final String USERNAME = "prometheus.auth.username";
  public static final String PASSWORD = "prometheus.auth.password";
  public static final String REGION = "prometheus.auth.region";
  public static final String ACCESS_KEY = "prometheus.auth.access_key";
  public static final String SECRET_KEY = "prometheus.auth.secret_key";

  private static final Logger LOG = LogManager.getLogger();

  private final org.opensearch.sql.opensearch.setting.OpenSearchSettings settings;
  private final DataSourceService dataSourceService;

  @Inject
  public DataSourceClientFactory(DataSourceService dataSourceService,
                                org.opensearch.sql.common.setting.Settings settings) {
    this.settings = (org.opensearch.sql.opensearch.setting.OpenSearchSettings) settings;
    this.dataSourceService = dataSourceService;
  }

  /**
   * Creates a client for the specified data source with appropriate type.
   *
   * @param <T> The type of client to create
   * @param dataSourceName The name of the data source
   * @return The appropriate client for the data source type
   * @throws DataSourceClientException If client creation fails
   */
  @SuppressWarnings("unchecked")
  public <T> T createClient(String dataSourceName) throws DataSourceClientException {
    try {
      if (!dataSourceService.dataSourceExists(dataSourceName)) {
        throw new DataSourceClientException("Data source does not exist: " + dataSourceName);
      }

      DataSourceMetadata metadata = dataSourceService.getDataSourceMetadata(dataSourceName);
      DataSourceType dataSourceType = metadata.getConnector();

      return (T) createClientForType(dataSourceType.name(), metadata);
    } catch (Exception e) {
      if (e instanceof DataSourceClientException) {
        throw e;
      }
      LOG.error("Failed to create client for data source: " + dataSourceName, e);
      throw new DataSourceClientException("Failed to create client for data source: " + dataSourceName, e);
    }
  }

  private Object createClientForType(String dataSourceType, DataSourceMetadata metadata) throws DataSourceClientException {
    switch (dataSourceType) {
      case "PROMETHEUS":
        return createPrometheusClient(metadata);
      // Add cases for other data source types as needed
      default:
        throw new DataSourceClientException("Unsupported data source type: " + dataSourceType);
    }
  }

  // TODO: Move this to a common place for this file and PrometheusStorageFactory
  private PrometheusClient createPrometheusClient(DataSourceMetadata metadata) {
    try {
      // replace this with validate properties in PrometheusStorageFactory
      String host = metadata.getProperties().get(URI);
      if (Objects.isNull(host)) {
        throw new DataSourceClientException("Host is required for Prometheus data source");
      }

      URI uri = new URI(host);
      return new PrometheusClientImpl(getHttpClient(metadata.getProperties()), uri);
    } catch (URISyntaxException e) {
      throw new DataSourceClientException("Invalid Prometheus URI", e);
    }
  }

  private OkHttpClient getHttpClient(Map<String, String> config) {
    return SecurityAccess.doPrivileged(() -> {
        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
        okHttpClient.callTimeout(1, TimeUnit.MINUTES);
        okHttpClient.connectTimeout(30, TimeUnit.SECONDS);
        okHttpClient.followRedirects(false);
        okHttpClient.addInterceptor(
            new URIValidatorInterceptor(
                settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST)));
        if (config.get(AUTH_TYPE) != null) {
            AuthenticationType authenticationType = AuthenticationType.get(config.get(AUTH_TYPE));
            if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
                okHttpClient.addInterceptor(
                    new BasicAuthenticationInterceptor(config.get(USERNAME), config.get(PASSWORD)));
            } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
                okHttpClient.addInterceptor(
                    new AwsSigningInterceptor(
                        new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(config.get(ACCESS_KEY), config.get(SECRET_KEY))),
                        config.get(REGION),
                        "aps"));
            } else {
                throw new IllegalArgumentException(
                    String.format(
                        "AUTH Type : %s is not supported with Prometheus Connector",
                        config.get(AUTH_TYPE)));
            }
        }
        return okHttpClient.build();
    });
  }
}
