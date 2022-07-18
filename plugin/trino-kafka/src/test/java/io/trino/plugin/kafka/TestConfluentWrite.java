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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestConfluentWrite
        extends AbstractTestQueryFramework
{
    @Test
    public void testQuery()
    {
        MaterializedResult res = computeActual("SELECT * FROM kafka.default.risk");
        assertEquals(99, res.getRowCount());
    }

    @Test
    public void testInsert()
    {
        computeActual("INSERT INTO kafka.default.risk SELECT * FROM kafka.default.risk WHERE _key in ('UIPID:0','UIPID:1','UIPID:2')");
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        DistributedQueryRunner qrunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(1)
                .setExtraProperties(ImmutableMap.of())
                .build();

        qrunner.installPlugin(new KafkaPlugin());
        qrunner.createCatalog(
                "kafka",
                "kafka",
                ImmutableMap.<String, String>builder()
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.table-description-supplier", "CONFLUENT")
                        .put("kafka.confluent-schema-registry-url", "http://localhost:8081")
                        .buildOrThrow());

        return qrunner;
    }

    private static Session createSession(String schema)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog("kafka")
                .setSchema(schema)
                .build();
    }
}
