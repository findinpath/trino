/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.NodeVersion;
import org.apache.iceberg.CatalogProperties;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.AUTH_SESSION_TIMEOUT_MS;

public class IcebergRestCatalogPropertiesProvider
{
    private final URI serverUri;
    private final Optional<String> prefix;
    private final Optional<String> warehouse;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> socketTimeout;
    private final Duration sessionTimeout;
    private final boolean vendedCredentialsEnabled;
    private final boolean viewEndpointsEnabled;
    private final SecurityProperties securityProperties;

    private final String trinoVersion;

    private final ImmutableMap<String, String> catalogProperties;

    @Inject
    public IcebergRestCatalogPropertiesProvider(
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            NodeVersion nodeVersion)
    {
        requireNonNull(restConfig, "restConfig is null");

        this.serverUri = restConfig.getBaseUri();
        this.prefix = restConfig.getPrefix();
        this.warehouse = restConfig.getWarehouse();
        this.connectionTimeout = restConfig.getConnectionTimeout();
        this.socketTimeout = restConfig.getSocketTimeout();
        this.sessionTimeout = restConfig.getSessionTimeout();
        this.vendedCredentialsEnabled = restConfig.isVendedCredentialsEnabled();
        this.viewEndpointsEnabled = restConfig.isViewEndpointsEnabled();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();

        catalogProperties = createCatalogProperties();
    }

    private ImmutableMap<String, String> createCatalogProperties()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, serverUri.toString());
        warehouse.ifPresent(location -> properties.put(CatalogProperties.WAREHOUSE_LOCATION, location));
        prefix.ifPresent(prefix -> properties.put("prefix", prefix));
        properties.put("view-endpoints-supported", Boolean.toString(viewEndpointsEnabled));
        properties.put("trino-version", trinoVersion);
        properties.put(AUTH_SESSION_TIMEOUT_MS, String.valueOf(sessionTimeout.toMillis()));
        connectionTimeout.ifPresent(duration -> properties.put("rest.client.connection-timeout-ms", String.valueOf(duration.toMillis())));
        socketTimeout.ifPresent(duration -> properties.put("rest.client.socket-timeout-ms", String.valueOf(duration.toMillis())));
        properties.putAll(securityProperties.get());

        if (vendedCredentialsEnabled) {
            properties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        }
        return properties.buildOrThrow();
    }

    public Map<String, String> getCatalogProperties()
    {
        return catalogProperties;
    }
}
