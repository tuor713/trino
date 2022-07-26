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

import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.avro.AvroRowEncoder;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import javax.validation.ValidationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ConfluentAvroRowEncoder
        extends AvroRowEncoder
{
    private final int schemaId;

    public ConfluentAvroRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema parsedSchema, int schemaId)
    {
        super(session, columnHandles, parsedSchema);
        this.schemaId = schemaId;
    }

    @Override
    public byte[] toByteArray()
    {
        try {
            if (!GenericData.get().validate(record.getSchema(), record)) {
                throw new ValidationException("Record " + record + " does not match schema " + record.getSchema());
            }

            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            bout.write(0);
            bout.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder enc = EncoderFactory.get().directBinaryEncoder(bout, null);
            GenericDatumWriter writer = new GenericDatumWriter(parsedSchema);
            writer.write(record, enc);
            enc.flush();

            // required otherwise encoder throws an error
            resetColumnIndex();

            return bout.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
