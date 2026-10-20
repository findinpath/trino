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

import static java.util.Objects.requireNonNull;

/**
 * The relations a materialized view query depends on, as resolved by the engine when analyzing the query.
 *
 * @param sources the relations referenced directly by the materialized view query
 * @param hasSourceTableFunctions whether the query invokes table functions
 * @param hasNonDeterministicFunctions whether the query invokes non-deterministic functions
 */
public record MaterializedViewSources(List<MaterializedViewSource> sources, boolean hasSourceTableFunctions, boolean hasNonDeterministicFunctions)
{
    public MaterializedViewSources
    {
        sources = List.copyOf(requireNonNull(sources, "sources is null"));
    }
}
