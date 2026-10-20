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
package io.trino.spi.connector;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class MaterializedViewFreshness
{
    private final Freshness freshness;
    private final Optional<Instant> lastKnownFreshTime;
    private final boolean requiresSources;

    public MaterializedViewFreshness(Freshness freshness, Optional<Instant> lastKnownFreshTime)
    {
        this(freshness, lastKnownFreshTime, false);
    }

    public MaterializedViewFreshness(Freshness freshness, Optional<Instant> lastKnownFreshTime, boolean requiresSources)
    {
        this.freshness = requireNonNull(freshness, "freshness is null");
        this.lastKnownFreshTime = requireNonNull(lastKnownFreshTime, "lastKnownFreshTime is null");
        if (requiresSources && freshness != Freshness.UNKNOWN) {
            throw new IllegalArgumentException("requiresSources can only be set for UNKNOWN freshness");
        }
        this.requiresSources = requiresSources;
    }

    public Freshness getFreshness()
    {
        return freshness;
    }

    /**
     * Last time when the materialized view was known to be fresh.
     */
    public Optional<Instant> getLastKnownFreshTime()
    {
        return lastKnownFreshTime;
    }

    /**
     * Whether the connector could determine the freshness if the engine provided the relations the
     * materialized view query depends on, through
     * {@link ConnectorMetadata#getMaterializedViewFreshness(ConnectorSession, SchemaTableName, boolean, MaterializedViewSources)}.
     * Only set together with {@link Freshness#UNKNOWN}.
     */
    public boolean requiresSources()
    {
        return requiresSources;
    }

    /**
     * @deprecated Use {@link #getLastKnownFreshTime()} instead.
     */
    @Deprecated
    public Optional<Instant> getLastFreshTime()
    {
        return getLastKnownFreshTime();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedViewFreshness that = (MaterializedViewFreshness) obj;
        return freshness == that.freshness
                && Objects.equals(lastKnownFreshTime, that.lastKnownFreshTime)
                && requiresSources == that.requiresSources;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(freshness, lastKnownFreshTime, requiresSources);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", MaterializedViewFreshness.class.getSimpleName() + "[", "]")
                .add("freshness=" + freshness)
                .add("lastKnownFreshTime=" + lastKnownFreshTime.orElse(null))
                .add("requiresSources=" + requiresSources)
                .toString();
    }

    public enum Freshness
    {
        FRESH,
        /**
         * The materialized view is fresh enough within its grace period. Returned when a connector
         * is invoked with {@code considerGracePeriod = true} and determined it could skip expensive
         * freshness checks (e.g., checking base table snapshots) because the MV is still within
         * the grace period bounds. The actual freshness of base tables was not verified, but worst-case
         * staleness is guaranteed to be within the acceptable grace period.
         */
        FRESH_WITHIN_GRACE_PERIOD,
        STALE,
        UNKNOWN,
    }
}
