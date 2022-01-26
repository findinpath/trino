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

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A lock that can be unlocked in a different thread.
 */
final class TestingResourceLock<ResourceToken>
{
    private final Semaphore semaphore = new Semaphore(1);

    private final AtomicReference<Optional<Exception>> lockHolder = new AtomicReference<>(Optional.empty());

    public ResourceLock<ResourceToken> lock()
    {
        try {
            boolean acquired = semaphore.tryAcquire(1, 120, MINUTES);
            checkState(acquired, "Unable to aquire semaphore; locked by: " + getLockHolderAsString());
        }
        catch (InterruptedException e) {
            currentThread().interrupt();
            throw new RuntimeException(e);
        }
        lockHolder.set(Optional.of(new Exception()));

        return new ResourceLock<ResourceToken>()
        {
            @GuardedBy("this")
            private boolean released;

            @Override
            public synchronized void close()
            {
                checkState(!released, "already released");
                released = true;
                lockHolder.set(Optional.empty());
                semaphore.release();
            }
        };
    }

    private String getLockHolderAsString()
    {
        Optional<Exception> holderException = lockHolder.get();
        if (holderException.isEmpty()) {
            return "UNKNOWN";
        }
        return getStackTraceAsString(holderException.get());
    }

    public abstract static class ResourceLock<ResourceToken>
            implements AutoCloseable
    {
        private ResourceLock() {}

        @Override
        public abstract void close();
    }
}
