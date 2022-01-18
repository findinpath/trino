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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import io.airlift.json.JsonCodec;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_VERSION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestingYugabyteServer
        implements Closeable
{
    public static final Integer PORT = 9042;
    public static final String SCHEMA = "tpch";

    private final GenericContainer<?> dockerContainer;
    private final CassandraSession session;

    public TestingYugabyteServer()
    {
        this("yugabytedb/yugabyte:latest");
    }

    public TestingYugabyteServer(String dockerImageName)
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse(dockerImageName))
                .withCommand("bin/yugabyted", "start", "--daemon=false")
                .withStartupTimeout(Duration.ofMinutes(3))
                .withExposedPorts(PORT);
        dockerContainer.start();

        ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withString(PROTOCOL_VERSION, ProtocolVersion.V3.name());
        driverConfigLoaderBuilder.withDuration(CONTROL_CONNECTION_AGREEMENT_TIMEOUT, java.time.Duration.ofSeconds(30));
        // allow the retrieval of metadata for the system keyspaces
        driverConfigLoaderBuilder.withStringList(METADATA_SCHEMA_REFRESHED_KEYSPACES, List.of());

        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
                .withApplicationName("TestCluster")
                .addContactPoint(new InetSocketAddress(this.dockerContainer.getContainerIpAddress(), this.dockerContainer.getMappedPort(PORT)))
                .withLocalDatacenter("datacenter1")
                .withConfigLoader(driverConfigLoaderBuilder.build());

        session = new CassandraSession(
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                cqlSessionBuilder::build,
                new io.airlift.units.Duration(Long.MAX_VALUE, DAYS),
                new io.airlift.units.Duration(1, MINUTES));
    }

    public CassandraSession getSession()
    {
        return requireNonNull(session, "session is null");
    }

    public String getHost()
    {
        return dockerContainer.getHost();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(PORT);
    }

    @Override
    public void close()
            throws IOException
    {
        dockerContainer.close();
    }
}
