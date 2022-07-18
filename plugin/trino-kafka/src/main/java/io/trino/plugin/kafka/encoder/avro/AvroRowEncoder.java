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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.AbstractRowEncoder;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroRowEncoder
        extends AbstractRowEncoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL);

    public static final String NAME = "avro";

    private final ByteArrayOutputStream byteArrayOutputStream;
    protected final Schema parsedSchema;
    private final DataFileWriter<GenericRecord> dataFileWriter;
    protected final GenericRecord record;

    public AvroRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema parsedSchema)
    {
        super(session, columnHandles);
        for (EncoderColumnHandle columnHandle : this.columnHandles) {
            checkArgument(columnHandle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
            checkArgument(columnHandle.getDataFormat() == null, "Unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());

            checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.parsedSchema = requireNonNull(parsedSchema, "parsedSchema is null");
        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(this.parsedSchema));
        this.record = new GenericData.Record(this.parsedSchema);
    }

    private static boolean isSupportedType(Type type)
    {
        return type instanceof VarcharType
                || type instanceof VarbinaryType
                || SUPPORTED_PRIMITIVE_TYPES.contains(type)
                || (type instanceof ArrayType && isSupportedType(((ArrayType) type).getElementType()))
                || (type instanceof RowType && ((RowType) type).getFields().stream().allMatch(f -> isSupportedType(f.getType())))
                || (type instanceof MapType
                    && ((MapType) type).getKeyType() instanceof VarcharType
                    && isSupportedType(((MapType) type).getValueType()));
    }

    private String currentColumnMapping()
    {
        return columnHandles.get(currentColumnIndex).getMapping();
    }

    @Override
    protected void appendNullValue()
    {
        record.put(currentColumnMapping(), null);
    }

    @Override
    protected void appendLong(long value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendInt(int value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendShort(short value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendByte(byte value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendDouble(double value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendFloat(float value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        record.put(currentColumnMapping(), value);
    }

    @Override
    protected void appendString(String value)
    {
        // Enum gets mapped to varchar, so check whether the type is Avro string or enum
        Schema fieldSchema = stripNullableUnion(parsedSchema.getField(currentColumnMapping()).schema());
        if (fieldSchema.getType() == Schema.Type.ENUM) {
            record.put(currentColumnMapping(), new GenericData.EnumSymbol(fieldSchema, value));
        }
        else {
            record.put(currentColumnMapping(), value);
        }
    }

    @Override
    protected void appendByteBuffer(ByteBuffer buffer)
    {
        Schema schema = stripNullableUnion(parsedSchema.getField(currentColumnMapping()).schema());
        if (schema.getType() == Schema.Type.FIXED) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            record.put(currentColumnMapping(), new GenericData.Fixed(schema, bytes));
        }
        else {
            // bytes type
            record.put(currentColumnMapping(), buffer);
        }
    }

    private Schema stripNullableUnion(Schema schema)
    {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> types = schema.getTypes();
            if (types.size() == 2) {
                if (types.get(0).getType() == Schema.Type.NULL) {
                    return types.get(1);
                }
                else if (types.get(1).getType() == Schema.Type.NULL) {
                    return types.get(0);
                }
            }
        }

        return schema;
    }

    @Override
    protected void appendArray(List<Object> value)
    {
        Schema fieldSchema = stripNullableUnion(parsedSchema.getField(currentColumnMapping()).schema());
        NestedEncoder encoder = createNestedEncoder(columnHandles.get(currentColumnIndex).getType(), fieldSchema);
        record.put(currentColumnMapping(), encoder.encode(value));
    }

    @Override
    protected void appendMap(Map<Object, Object> value)
    {
        Schema curSchema = stripNullableUnion(parsedSchema.getField(currentColumnMapping()).schema());
        NestedEncoder encoder = createNestedEncoder(columnHandles.get(currentColumnIndex).getType(), curSchema);
        record.put(currentColumnMapping(), encoder.encode(value));
    }

    private interface NestedEncoder
    {
        Object encode(Object value);
    }

    private static final NestedEncoder IDENTITY_ENCODER = value -> value;

    private NestedEncoder createNestedEncoder(Type trinoType, Schema schema)
    {
        if (schema.getType() == Schema.Type.RECORD) {
            AvroRowEncoder encoder = createNestedRowEncoder((RowType) trinoType, schema);
            return value -> {
                for (Object colVal : (List) value) {
                    encoder.appendColumnValue(colVal);
                }
                encoder.resetColumnIndex();
                return encoder.record;
            };
        }
        else if (schema.getType() == Schema.Type.BYTES) {
            return value -> {
                byte[] bytes = ((SqlVarbinary) value).getBytes();
                return ByteBuffer.wrap(bytes);
            };
        }
        else if (schema.getType() == Schema.Type.ENUM) {
            return value -> new GenericData.EnumSymbol(schema, (String) value);
        }
        else if (schema.getType() == Schema.Type.FIXED) {
            return value -> {
                byte[] bytes = ((SqlVarbinary) value).getBytes();
                return new GenericData.Fixed(schema, bytes);
            };
        }
        else if (schema.getType() == Schema.Type.ARRAY) {
            return value -> {
                Schema itemSchema = schema.getElementType();
                NestedEncoder encoder = createNestedEncoder(((ArrayType) trinoType).getElementType(), itemSchema);
                return ((List<Object>) value).stream().map(item -> encoder.encode(item)).collect(Collectors.toList());
            };
        }
        else if (schema.getType() == Schema.Type.MAP) {
            return value -> {
                Schema valSchema = schema.getValueType();
                Map<Object, Object> res = new HashMap<>();
                NestedEncoder valEncoder = createNestedEncoder(
                        ((MapType) trinoType).getValueType(),
                        valSchema);

                Map<String, Object> map = (Map<String, Object>) value;
                for (Map.Entry<String, Object> e : map.entrySet()) {
                    res.put(e.getKey(), valEncoder.encode(e.getValue()));
                }
                return res;
            };
        }
        else {
            return IDENTITY_ENCODER;
        }
    }

    private AvroRowEncoder createNestedRowEncoder(RowType type, Schema schema)
    {
        List<RowType.Field> trinoFields = type.getFields();
        List<Schema.Field> avroFields = schema.getFields();
        checkArgument(trinoFields.size() == avroFields.size());

        List<EncoderColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < trinoFields.size(); i++) {
            columns.add(new KafkaColumnHandle(
                    avroFields.get(i).name(),
                    trinoFields.get(i).getType(),
                    avroFields.get(i).name(),
                    null, null, false, false, false));
        }

        return new AvroRowEncoder(session, columns, schema);
    }

    @Override
    protected void appendRow(List<Object> value)
    {
        Schema nestedSchema = stripNullableUnion(parsedSchema.getField(currentColumnMapping()).schema());
        NestedEncoder encoder = createNestedEncoder(columnHandles.get(currentColumnIndex).getType(), nestedSchema);
        record.put(currentColumnMapping(), encoder.encode(value));
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex));

        try {
            byteArrayOutputStream.reset();
            dataFileWriter.create(parsedSchema, byteArrayOutputStream);
            dataFileWriter.append(record);
            dataFileWriter.close();

            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to append record", e);
        }
    }

    private void appendColumnValue(Object value)
    {
        Type type = columnHandles.get(currentColumnIndex).getType();
        if (value == null) {
            appendNullValue();
        }
        else if (type == BOOLEAN) {
            appendBoolean((Boolean) value);
        }
        else if (type == BIGINT) {
            appendLong((Long) value);
        }
        else if (type == INTEGER) {
            appendInt((Integer) value);
        }
        else if (type == SMALLINT) {
            appendShort((Short) value);
        }
        else if (type == TINYINT) {
            appendByte((Byte) value);
        }
        else if (type == DOUBLE) {
            appendDouble((Double) value);
        }
        else if (type == REAL) {
            appendFloat((Float) value);
        }
        else if (type instanceof VarcharType) {
            appendString((String) value);
        }
        else if (type instanceof VarbinaryType) {
            appendByteBuffer((ByteBuffer) value);
        }
        else if (type == DATE) {
            appendSqlDate((SqlDate) value);
        }
        else if (type instanceof TimeType) {
            appendSqlTime((SqlTime) value);
        }
        else if (type instanceof TimeWithTimeZoneType) {
            appendSqlTimeWithTimeZone((SqlTimeWithTimeZone) value);
        }
        else if (type instanceof TimestampType) {
            appendSqlTimestamp((SqlTimestamp) value);
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            appendSqlTimestampWithTimeZone((SqlTimestampWithTimeZone) value);
        }
        else if (type instanceof ArrayType) {
            appendArray((List<Object>) value);
        }
        else if (type instanceof MapType) {
            appendMap((Map<Object, Object>) value);
        }
        else if (type instanceof RowType) {
            appendRow((List<Object>) value);
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", type, columnHandles.get(currentColumnIndex).getName()));
        }
        currentColumnIndex++;
    }

    @Override
    public void close()
    {
        try {
            byteArrayOutputStream.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close ByteArrayOutputStream", e);
        }
    }
}
