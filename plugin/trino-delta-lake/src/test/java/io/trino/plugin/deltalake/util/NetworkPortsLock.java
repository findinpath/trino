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

import io.trino.plugin.deltalake.util.TestingResourceLock.ResourceLock;

/*
 * Tests that depend on hard coded system ports can break each other when run in parallel.
 * In order to avoid this, they should acquire the lock so they could be run serially.
 */
final class NetworkPortsLock
{
    private NetworkPortsLock() {}

    private static final TestingResourceLock<PortLock> INSTANCE = new TestingResourceLock<>();

    public static ResourceLock<PortLock> lockNetworkPorts()
    {
        return INSTANCE.lock();
    }

    public interface PortLock {}
}
