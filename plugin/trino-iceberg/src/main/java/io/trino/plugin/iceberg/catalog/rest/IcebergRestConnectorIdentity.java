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
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.SelectedRole;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.rest.RESTUtil;

import java.security.Principal;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class IcebergRestConnectorIdentity
        extends ConnectorIdentity
{
    static final long PREFETCH_MINUTES = 5;

    private final Map<String, String> catalogProperties;
    private final Map<String, String> fileIoProperties;
    private final Map<String, String> properties;
    private final String catalogEndpoint;
    private final boolean refreshCredentialsEnabled;
    private final Optional<String> credentialsEndpoint;

    protected IcebergRestConnectorIdentity(
            String user,
            Set<String> groups,
            Optional<Principal> principal,
            Set<String> enabledSystemRoles,
            Optional<SelectedRole> connectorRole,
            Map<String, String> extraCredentials,
            Map<String, String> catalogProperties,
            Map<String, String> fileIoProperties)
    {
        super(user, groups, principal, enabledSystemRoles, connectorRole, extraCredentials);
        this.catalogProperties = ImmutableMap.copyOf(requireNonNull(catalogProperties, "catalogProperties is null"));
        this.fileIoProperties = ImmutableMap.copyOf(requireNonNull(fileIoProperties, "fileIoProperties is null"));

        this.properties = RESTUtil.merge(catalogProperties, fileIoProperties);
        this.catalogEndpoint = requireNonNull(properties.get(CatalogProperties.URI), "catalog endpoint is null");
        this.refreshCredentialsEnabled = parseBoolean(fileIoProperties, AwsClientProperties.REFRESH_CREDENTIALS_ENABLED, true) &&
                parseBoolean(fileIoProperties, AzureProperties.ADLS_REFRESH_CREDENTIALS_ENABLED, true) &&
                parseBoolean(fileIoProperties, GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, true);
        this.credentialsEndpoint = requireNonNull(credentialsEndpoint, "credentialsEndpoint is null");
        this.initialCredentials = requireNonNull(initialCredentials, "initialCredentials is null");
        this.cached = new CachedState(initialCredentials, requireNonNull(initialExpiresAt, "initialExpiresAt is null"));
    }


    protected static boolean parseBoolean(Map<String, String> properties, String key, boolean defaultValue)
    {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }
        throw new IllegalArgumentException("Invalid boolean value for property '%s': %s".formatted(key, value));
    }

    protected static Optional<Instant> parseInstantEpochMillis(Map<String, String> properties, String key)
    {
        String value = properties.get(key);
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.ofEpochMilli(Long.parseLong(value)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid epoch millis value for property '%s': %s".formatted(key, value), e);
        }
    }

    public static Builder forIdentityUser(String user)
    {
        return new Builder(user);
    }

    public static class Builder
    {
        private final String user;
        private Set<String> groups = emptySet();
        private Optional<Principal> principal = Optional.empty();
        private Set<String> enabledSystemRoles = new HashSet<>();
        private Optional<SelectedRole> connectorRole = Optional.empty();
        private Map<String, String> extraCredentials = new HashMap<>();
        private Map<String, String> catalogProperties = new HashMap<>();
        private Map<String, String> fileIoProperties = new HashMap<>();

        private Builder(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        public Builder withGroups(Set<String> groups)
        {
            this.groups = Set.copyOf(requireNonNull(groups, "groups is null"));
            return this;
        }

        public Builder withPrincipal(Principal principal)
        {
            return withPrincipal(Optional.of(requireNonNull(principal, "principal is null")));
        }

        public Builder withPrincipal(Optional<Principal> principal)
        {
            this.principal = requireNonNull(principal, "principal is null");
            return this;
        }

        public Builder withEnabledSystemRoles(Set<String> enabledSystemRoles)
        {
            this.enabledSystemRoles = new HashSet<>(requireNonNull(enabledSystemRoles, "enabledSystemRoles is null"));
            return this;
        }

        public Builder withConnectorRole(SelectedRole connectorRole)
        {
            return withConnectorRole(Optional.of(requireNonNull(connectorRole, "connectorRole is null")));
        }

        public Builder withConnectorRole(Optional<SelectedRole> connectorRole)
        {
            this.connectorRole = requireNonNull(connectorRole, "connectorRole is null");
            return this;
        }

        public Builder withExtraCredentials(Map<String, String> extraCredentials)
        {
            this.extraCredentials = new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null"));
            return this;
        }

        public Builder withCatalogProperties(Map<String, String> catalogProperties)
        {
            this.catalogProperties = new HashMap<>(requireNonNull(catalogProperties, "catalogProperties is null"));
            return this;
        }

        public Builder withFileIoProperties(Map<String, String> fileIoProperties)
        {
            this.fileIoProperties = new HashMap<>(requireNonNull(fileIoProperties, "fileIoProperties is null"));
            return this;
        }

        public IcebergRestConnectorIdentity build()
        {
            return new IcebergRestConnectorIdentity(
                    user,
                    groups,
                    principal,
                    enabledSystemRoles,
                    connectorRole,
                    extraCredentials,
                    catalogProperties,
                    fileIoProperties);
        }
    }
}
