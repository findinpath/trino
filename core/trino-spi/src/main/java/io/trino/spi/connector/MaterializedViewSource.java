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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A relation the materialized view query depends on, as resolved by the engine when analyzing the query.
 * Views and materialized views are always expanded, so each of them carries the relations its own query
 * depends on.
 */
public sealed interface MaterializedViewSource
        permits MaterializedViewSource.Table,
                MaterializedViewSource.View,
                MaterializedViewSource.MaterializedView,
                MaterializedViewSource.Foreign
{
    record Table(ConnectorTableHandle handle)
            implements MaterializedViewSource
    {
        public Table
        {
            requireNonNull(handle, "handle is null");
        }
    }

    record View(ConnectorViewHandle handle, List<MaterializedViewSource> sources)
            implements MaterializedViewSource
    {
        public View
        {
            requireNonNull(handle, "handle is null");
            sources = List.copyOf(requireNonNull(sources, "sources is null"));
        }
    }

    /**
     * @param storageTable the storage table of the materialized view, if it belongs to the same catalog
     * @param sources the relations the query of the materialized view depends on
     */
    record MaterializedView(ConnectorViewHandle handle, Optional<ConnectorTableHandle> storageTable, List<MaterializedViewSource> sources)
            implements MaterializedViewSource
    {
        public MaterializedView
        {
            requireNonNull(handle, "handle is null");
            requireNonNull(storageTable, "storageTable is null");
            sources = List.copyOf(requireNonNull(sources, "sources is null"));
        }
    }

    /**
     * A relation that belongs to a different catalog than the materialized view.
     */
    record Foreign(CatalogSchemaTableName name)
            implements MaterializedViewSource
    {
        public Foreign
        {
            requireNonNull(name, "name is null");
        }
    }
}
