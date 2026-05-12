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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveLocalInsertOverwrite
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA_NAME = "tpch";

    private Path dataDirectory;
    private FileHiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("test_insert_overwrite");
        // Using FileHiveMetastore as approximation of HMS
        metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                new LocalFileSystemFactory(Path.of(dataDirectory.toString())),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local://"));

        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TestingHivePlugin(Path.of(dataDirectory.toString()), metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of("fs.hadoop.enabled", "true"));
        metastore.createDatabase(createDatabaseMetastoreObject(SCHEMA_NAME, Optional.empty()));

        return queryRunner;
    }

    private static Database createDatabaseMetastoreObject(String name, Optional<String> locationBase)
    {
        return Database.builder()
                .setLocation(locationBase.map(base -> base + "/" + name))
                .setDatabaseName(name)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA_NAME, true);
        }
        if (dataDirectory != null) {
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }

    @Test
    void testInsertOverwriteTableUnion()
    {
        String tableName = "test_insert_overwrite_" + randomNameSuffix();
        String stagingTableName = "test_staging_" + randomNameSuffix();

        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INT, part INT) WITH (partitioned_by = ARRAY['part'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (11,10), (21,20)", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 1)", 1);

            assertUpdate("CREATE TABLE " + stagingTableName + " (id INT, part INT) WITH (partitioned_by = ARRAY['part'])");
            assertUpdate("INSERT INTO " + stagingTableName + " VALUES (3, 1)", 1);

            Session insertOverwriteSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();

            // Simulate a timeout exception while updating the 2nd partition
            metastore.setThrowTimeoutExceptionOnAlterPartitionCall(2);
            assertThatThrownBy(() -> assertUpdate(insertOverwriteSession, "INSERT INTO " + tableName + " SELECT * FROM " + tableName + " UNION SELECT * FROM " + stagingTableName))
                    .hasMessageContaining("timeout exception");
            metastore.setThrowTimeoutExceptionOnAlterPartitionCall(-1);

            // The content of the partition "part=1" is not available anymore
            assertQuery("SELECT * FROM " + tableName, "VALUES (11, 10), (21, 20)");
            assertQuery("SELECT part FROM \"" + tableName + "$partitions\"", "VALUES 1, 10, 20");

            assertUpdate(insertOverwriteSession, "INSERT INTO " + tableName + " SELECT * FROM " + tableName + " UNION SELECT * FROM " + stagingTableName, 3);
            assertQuery("SELECT * FROM " + tableName, "VALUES (3, 1), (11, 10), (21, 20)");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
            getQueryRunner().execute("DROP TABLE IF EXISTS " + stagingTableName);
        }
    }
}
