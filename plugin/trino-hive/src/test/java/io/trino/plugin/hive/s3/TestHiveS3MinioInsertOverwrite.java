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
package io.trino.plugin.hive.s3;

import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveS3MinioInsertOverwrite
        extends AbstractTestQueryFramework
{
    private Hive3MinioDataLake hiveMinioDataLake;
    private String bucketName;
    private AtomicReference<BridgingHiveMetastore> bridgingHiveMetastoreHolder = new AtomicReference<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-hive-minio-queries-" + randomNameSuffix();
        this.hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName));
        this.hiveMinioDataLake.start();

        return HiveQueryRunner.builder()
                .addHiveProperty("fs.s3.enabled", "true")
                .addHiveProperty("s3.region", MINIO_REGION)
                .addHiveProperty("s3.endpoint", "http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                .addHiveProperty("s3.aws-access-key", MINIO_ROOT_USER)
                .addHiveProperty("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                .addHiveProperty("s3.path-style-access", "true")
                .setMetastore(distributedQueryRunner -> {
                    var metastore = new BridgingHiveMetastore(
                            testingThriftHiveMetastoreBuilder()
                                    .metastoreClient(hiveMinioDataLake.getHiveMetastoreEndpoint(), TestingTokenAwareMetastoreClientFactory.TIMEOUT)
                                    .thriftMetastoreConfig(new ThriftMetastoreConfig())
                                    .build(distributedQueryRunner::registerResource));
                    bridgingHiveMetastoreHolder.set(metastore);
                    return metastore;
                })
                .setInitialSchemasLocationBase("s3a://" + bucketName)
                .build();
    }

    @Test
    void testInsertOverwriteTableUnion()
    {
        String tableName = "test_insert_overwrite_" + randomNameSuffix();
        String stagingTable = "test_staging_" + randomNameSuffix();

        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INT, part INT) WITH (partitioned_by = ARRAY['part'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (11,10), (21,20)", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 1)", 1);

            assertUpdate("CREATE TABLE " + stagingTable + " (id INT, part INT) WITH (partitioned_by = ARRAY['part'])");
            assertUpdate("INSERT INTO " + stagingTable + " VALUES (3, 1)", 1);

            Session insertOverwriteSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();

            // Simulate a timeout exception while updating the partition statistics of the table
            // as part of the logic of the method io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.commitShared
            var metastore = bridgingHiveMetastoreHolder.get();
            metastore.setThrowTimeoutExceptionOnUpdatePartitionStatistics(true);
            assertThatThrownBy(() -> assertUpdate(insertOverwriteSession, "INSERT INTO " + tableName + " SELECT * FROM " + tableName + " UNION SELECT * FROM " + stagingTable))
                    .hasMessageContaining("All operations other than the following update operations were completed: replace partition parameter");
            metastore.setThrowTimeoutExceptionOnUpdatePartitionStatistics(false);

            // All the content of the table is gone (both the files which existed before running the query,
            // as well as the files inserted by the insert overwrite query)
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);
            assertQuery("SELECT part FROM \"" + tableName + "$partitions\"", "VALUES 1, 10, 20");

            assertUpdate(insertOverwriteSession, "INSERT INTO " + tableName + " SELECT * FROM " + tableName + " UNION SELECT * FROM " + stagingTable, 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (3, 1)");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
