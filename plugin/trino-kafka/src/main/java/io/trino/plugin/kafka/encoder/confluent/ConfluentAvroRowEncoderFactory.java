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
package io.trino.plugin.kafka.encoder.confluent;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ConfluentAvroRowEncoderFactory
        implements RowEncoderFactory
{
    private SchemaRegistryClient client;

    @Inject
    public ConfluentAvroRowEncoderFactory(SchemaRegistryClient client)
    {
        this.client = client;
    }

    @Override
    public RowEncoder create(ConnectorSession session, Optional<String> subject, Optional<String> dataSchema, List<EncoderColumnHandle> columnHandles)
    {
        try {
            SchemaMetadata meta = client.getLatestSchemaMetadata(subject.orElseThrow());
            Schema schema = new Schema.Parser().parse(meta.getSchema());
            if (schema.getType() != Schema.Type.RECORD) {
                return new ConfluentAvroSingleColumnRowEncoder(session, columnHandles, schema, meta.getId());
            }
            else {
                return new ConfluentAvroRowEncoder(session, columnHandles, schema, meta.getId());
            }
        }
        catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }
}
