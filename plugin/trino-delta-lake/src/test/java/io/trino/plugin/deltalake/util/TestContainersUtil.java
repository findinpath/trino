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
package io.trino.plugin.deltalake.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.getenv;

final class TestContainersUtil
{
    // Please turn it on locally so container will survive JVM reboot.
    private static final boolean TESTCONTAINERS_REUSE_ENABLE = "true".equalsIgnoreCase(getenv("TESTCONTAINERS_REUSE_ENABLE"));

    static {
        TestcontainersConfiguration configuration = TestcontainersConfiguration.getInstance();
        if (configuration.environmentSupportsReuse() != TESTCONTAINERS_REUSE_ENABLE) {
            // Override configuration from ~/.testcontainers.properties
            configuration.updateGlobalConfig("testcontainers.reuse.enable", Boolean.toString(TESTCONTAINERS_REUSE_ENABLE));
        }
    }

    private TestContainersUtil() {}

    /**
     * You should not close the container directly if you want to reuse it. Instead you should close closeable returned by {@link startOrReuse}
     */
    public static Closeable startOrReuse(GenericContainer<?> container)
    {
        container.withReuse(TESTCONTAINERS_REUSE_ENABLE);
        container.start();
        if (TestcontainersConfiguration.getInstance().environmentSupportsReuse()) {
            return () -> {};
        }
        else {
            return container::stop;
        }
    }

    public static String getInternalIpAddress(GenericContainer<?> container)
    {
        checkState(container.isRunning(), "Container should be already running");
        return container.getContainerInfo().getNetworkSettings().getNetworks().get("bridge").getIpAddress();
    }
}
