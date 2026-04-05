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
package io.trino.filesystem.azure;

import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
import static java.util.Objects.requireNonNull;

public final class AzureAuthSasToken
        implements AzureAuth
{
    private final ConnectorIdentity identity;

    public AzureAuthSasToken(ConnectorIdentity identity)
    {
        this.identity = requireNonNull(identity, "identity is null");
    }

    @Override
    public void setAuth(String storageAccount, BlobContainerClientBuilder builder)
    {
        String sasToken = getSasToken(storageAccount);
        builder.sasToken(sasToken);
    }

    @Override
    public void setAuth(String storageAccount, DataLakeServiceClientBuilder builder)
    {
        String sasToken = getSasToken(storageAccount);
        builder.sasToken(sasToken);
    }

    private String getSasToken(String storageAccount)
    {
        Map<String, String> extraCredentials = identity.getExtraCredentials();
        Map<String, String> sasTokens = extraCredentials.entrySet().stream()
                .filter(e -> e.getKey().startsWith(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX))
                .collect(toImmutableMap(e -> e.getKey().substring(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX.length()), Map.Entry::getValue));

        String sasToken = sasTokens.get(storageAccount);
        if (sasToken == null) {
            throw new IllegalStateException("No SAS token provided for storage account: " + storageAccount);
        }
        return sasToken;
    }
}
