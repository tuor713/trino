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
package io.trino.plugin.kafka.encoder.avro;

import io.trino.plugin.kafka.encoder.AbstractRowEncoder;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class AvroSingleColumnRowEncoder
        extends AbstractRowEncoder
{
    private final Schema schema;
    private Object value;

    private final ByteArrayOutputStream byteArrayOutputStream;

    private final DataFileWriter dataFileWriter;

    public AvroSingleColumnRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema parseSchema)
    {
        super(session, columnHandles);
        if (columnHandles.size() > 1) {
            throw new UnsupportedOperationException("Non-Record Avro supports only single columns");
        }
        this.schema = parseSchema;
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.dataFileWriter = new DataFileWriter(new GenericDatumWriter(parseSchema));
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
            byteArrayOutputStream.reset();
            dataFileWriter.create(schema, byteArrayOutputStream);
            dataFileWriter.append(value);
            dataFileWriter.close();

            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to append record", e);
        }
    }
}
