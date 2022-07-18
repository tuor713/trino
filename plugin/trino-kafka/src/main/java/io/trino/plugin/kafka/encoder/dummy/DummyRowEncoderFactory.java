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
package io.trino.plugin.kafka.encoder.dummy;

import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class DummyRowEncoderFactory
        implements RowEncoderFactory
{
    @Override
    public RowEncoder create(ConnectorSession session, Optional<String> subject, Optional<String> dataSchema, List<EncoderColumnHandle> columnHandles)
    {
        if (columnHandles.size() > 1) {
            throw new UnsupportedOperationException("DummyRowEncoder handles only 0 or 1 columns");
        }
        if (!columnHandles.isEmpty() && !(columnHandles.get(0).getType() instanceof VarcharType)) {
            throw new UnsupportedOperationException("DummyRowEncoder handles only varchar column");
        }

        return new RowEncoder()
        {
            private byte[] result = new byte[0];

            @Override
            public void appendColumnValue(Block block, int position)
            {
                Type type = columnHandles.get(0).getType();
                String s = type.getSlice(block, position).toStringUtf8();
                result = s.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public byte[] toByteArray()
            {
                return result;
            }

            @Override
            public void close() {}
        };
    }
}
