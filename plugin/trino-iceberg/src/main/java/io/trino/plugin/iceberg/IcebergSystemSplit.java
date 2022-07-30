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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class IcebergSystemSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IcebergSystemSplit.class).instanceSize();

    private final List<HostAddress> addresses;

    public IcebergSystemSplit(HostAddress address)
    {
        this(ImmutableList.of(requireNonNull(address, "address is null")));
    }

    @JsonCreator
    public IcebergSystemSplit(@JsonProperty("addresses") List<HostAddress> addresses)
    {
        requireNonNull(addresses, "addresses is null");
        checkArgument(!addresses.isEmpty(), "addresses is empty");
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .toString();
    }
}
