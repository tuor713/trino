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

import io.trino.plugin.kafka.encoder.AbstractRowEncoder;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ConfluentAvroSingleColumnRowEncoder
        extends AbstractRowEncoder
{
    private final Schema schema;
    private final int schemaId;
    private Object value;

    public ConfluentAvroSingleColumnRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema parseSchema, int schemaId)
    {
        super(session, columnHandles);
        if (columnHandles.size() > 1) {
            throw new UnsupportedOperationException("Non-Record Avro supports only single columns");
        }
        this.schema = parseSchema;
        this.schemaId = schemaId;
    }

    @Override
    protected void appendString(String value)
    {
        this.value = value;
    }

    @Override
    public byte[] toByteArray()
    {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            bout.write(0);
            bout.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder enc = EncoderFactory.get().directBinaryEncoder(bout, null);
            GenericDatumWriter writer = new GenericDatumWriter(schema);
            writer.write(value, enc);
            enc.flush();

            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return bout.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to append record", e);
        }
    }
}
