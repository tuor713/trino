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

import io.airlift.slice.Slices;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleMapBlockWriter;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import javax.validation.ValidationException;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class TestAvroEncoder
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder().build();
    private static final AvroRowEncoderFactory ENCODER_FACTORY = new AvroRowEncoderFactory();

    @Test
    public void testComplexSchema() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("complex")
                .fields()
                .requiredInt("fInt")
                .name("fIntArray").type().array().items().intType().noDefault()
                .name("fNested")
                    .type()
                    .record("inner")
                    .fields()
                    .requiredInt("fInnerInt")
                    .requiredInt("fInnerInt2")
                    .endRecord()
                    .noDefault()
                .name("fEnum").type().enumeration("myEnum").symbols("ALPHA", "BETA", "GAMMA").noDefault()
                .name("fRecArray")
                    .type()
                    .array()
                    .items(SchemaBuilder.builder().record("innerAr").fields().requiredInt("fArInt").endRecord())
                    .noDefault()
                .name("fDoubleNested").type(
                        SchemaBuilder.builder().record("inner1").fields().name("fOneNested").type()
                                .record("inner2").fields().requiredInt("fTwoNested").endRecord().noDefault().endRecord()
                ).noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    Block fIntBlock = pb.getBlockBuilder(0).writeInt(42).build();
                    encoder.appendColumnValue(fIntBlock, 0);

                    BlockBuilder fIntArBlock = pb.getBlockBuilder(1);
                    BlockBuilder fIntArSub = fIntArBlock.beginBlockEntry();
                    fIntArSub.writeInt(1).writeInt(2);
                    fIntArBlock.closeEntry();
                    encoder.appendColumnValue(fIntArBlock, 0);

                    BlockBuilder fNested = pb.getBlockBuilder(2);
                    SingleRowBlockWriter fRowBuilder = (SingleRowBlockWriter) fNested.beginBlockEntry();
                    fRowBuilder.getFieldBlockBuilder(0).writeInt(142);
                    fRowBuilder.getFieldBlockBuilder(1).writeInt(143);
                    fNested.closeEntry();
                    encoder.appendColumnValue(fNested, 0);

                    BlockBuilder fEnum = pb.getBlockBuilder(3);
                    VarcharType.VARCHAR.writeString(fEnum, "ALPHA");
                    encoder.appendColumnValue(fEnum, 0);

                    BlockBuilder fRecArBlock = pb.getBlockBuilder(4);
                    BlockBuilder fRecArSub = fRecArBlock.beginBlockEntry();
                    fRowBuilder = (SingleRowBlockWriter) fRecArSub.beginBlockEntry();
                    fRowBuilder.getFieldBlockBuilder(0).writeInt(1000);
                    fRecArSub.closeEntry();
                    fRecArBlock.closeEntry();
                    encoder.appendColumnValue(fRecArBlock, 0);

                    BlockBuilder fDoubleNested = pb.getBlockBuilder(5);
                    fRowBuilder = (SingleRowBlockWriter) fDoubleNested.beginBlockEntry();
                    BlockBuilder fieldBuilder = fRowBuilder.getFieldBlockBuilder(0);
                    SingleRowBlockWriter innerRowBuilder = (SingleRowBlockWriter) fieldBuilder.beginBlockEntry();
                    innerRowBuilder.getFieldBlockBuilder(0).writeInt(12345);
                    fieldBuilder.closeEntry();
                    fDoubleNested.closeEntry();
                    encoder.appendColumnValue(fDoubleNested, 0);
                });
    }

    @Test
    public void testOptEnum() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .name("fOptEnum").type().unionOf().nullType().and().enumeration("myEnum").symbols("ALPHA", "BETA", "GAMMA").endUnion().nullDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder fEnum = pb.getBlockBuilder(0);
                    VarcharType.VARCHAR.writeString(fEnum, "ALPHA");
                    encoder.appendColumnValue(fEnum, 0);
                });
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testNotNullSchemaEnforcement() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .name("fString").type().stringType().noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder fString = pb.getBlockBuilder(0);
                    fString.appendNull();
                    encoder.appendColumnValue(fString, 0);
                });
    }

    @Test
    public void testNestedEnum() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("outer")
                .fields()
                .name("inner").type(
                        SchemaBuilder
                                .builder()
                                .record("inner")
                                .fields()
                                .name("f").type().enumeration("myEnum").symbols("PROD", "UAT", "DEV")
                                .noDefault()
                                .endRecord())
                .noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder fNested = pb.getBlockBuilder(0);
                    SingleRowBlockWriter fRowBuilder = (SingleRowBlockWriter) fNested.beginBlockEntry();
                    VarcharType.VARCHAR.writeString(fRowBuilder, "PROD");
                    fNested.closeEntry();
                    encoder.appendColumnValue(fNested, 0);
                });
    }

    @Test
    public void testOptRecord() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("outer")
                .fields()
                .name("fOptInner")
                .type()
                .unionOf()
                .nullType()
                .and()
                .record("inner")
                .fields()
                .requiredInt("fInt")
                .endRecord()
                .endUnion()
                .nullDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder fNested = pb.getBlockBuilder(0);
                    SingleRowBlockWriter fRowBuilder = (SingleRowBlockWriter) fNested.beginBlockEntry();
                    fRowBuilder.getFieldBlockBuilder(0).writeInt(142);
                    fNested.closeEntry();
                    encoder.appendColumnValue(fNested, 0);
                });
    }

    @Test
    public void testBytesType() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .name("f").type().bytesType()
                .noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder bb = pb.getBlockBuilder(0);
                    VarbinaryType.VARBINARY.writeSlice(bb, Slices.wrappedIntArray(1, 2, 3, 4));
                    encoder.appendColumnValue(bb, 0);
                });
    }

    @Test
    public void testFixedType() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .name("f").type().fixed("myFixed").size(3).noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder bb = pb.getBlockBuilder(0);
                    VarbinaryType.VARBINARY.writeSlice(bb, Slices.wrappedBuffer(new byte[]{1, 2, 3}, 0, 3));
                    encoder.appendColumnValue(bb, 0);
                });
    }

    @Test
    public void testMapType() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .name("f").type().map().values().intType().noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder bb = pb.getBlockBuilder(0);

                    SingleMapBlockWriter smbw = (SingleMapBlockWriter) bb.beginBlockEntry();
                    VarcharType.VARCHAR.writeString(smbw, "one");
                    smbw.writeInt(1);
                    smbw.closeEntry();

                    VarcharType.VARCHAR.writeString(smbw, "two");
                    smbw.writeInt(2);
                    smbw.closeEntry();
                    bb.closeEntry();

                    encoder.appendColumnValue(bb, 0);
                });
    }

    @Test
    public void testArrayOfRecords() throws Exception
    {
        Schema schema = SchemaBuilder.builder()
                .record("test")
                .fields()
                .name("fRecArray")
                .type()
                .array()
                .items(SchemaBuilder.builder().record("innerAr").fields().requiredInt("fArInt").endRecord())
                .noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder fRecArBlock = pb.getBlockBuilder(0);
                    BlockBuilder fRecArSub = fRecArBlock.beginBlockEntry();
                    SingleRowBlockWriter fRowBuilder = (SingleRowBlockWriter) fRecArSub.beginBlockEntry();
                    fRowBuilder.getFieldBlockBuilder(0).writeInt(1000);
                    fRecArSub.closeEntry();

                    fRowBuilder = (SingleRowBlockWriter) fRecArSub.beginBlockEntry();
                    fRowBuilder.getFieldBlockBuilder(0).writeInt(2000);
                    fRecArSub.closeEntry();

                    fRecArBlock.closeEntry();
                    encoder.appendColumnValue(fRecArBlock, 0);
                },
                (rec) -> {
                    List<Integer> entries = ((List<GenericRecord>) rec.get("fRecArray"))
                            .stream()
                            .map(r -> (Integer) r.get("fArInt"))
                            .collect(Collectors.toList());
                    assertEquals(List.of(1000, 2000), entries);
                });
    }

    @Test
    public void testMapOfBytes() throws Exception
    {
        Schema schema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                    .name("f").type().map().values().bytesType().noDefault()
                .endRecord();

        testSchemaConversion(schema,
                (pb, encoder) -> {
                    BlockBuilder bb = pb.getBlockBuilder(0);

                    SingleMapBlockWriter smbw = (SingleMapBlockWriter) bb.beginBlockEntry();
                    VarcharType.VARCHAR.writeString(smbw, "one");
                    VarbinaryType.VARBINARY.writeSlice(smbw, Slices.wrappedIntArray(1));
                    smbw.closeEntry();

                    VarcharType.VARCHAR.writeString(smbw, "two");
                    VarbinaryType.VARBINARY.writeSlice(smbw, Slices.wrappedIntArray(1, 2));
                    smbw.closeEntry();
                    bb.closeEntry();

                    encoder.appendColumnValue(bb, 0);
                });
    }

    private void testSchemaConversion(Schema schema, BiConsumer<PageBuilder, RowEncoder> dataGen) throws Exception
    {
        testSchemaConversion(schema, dataGen, (rec) -> {});
    }

    private void testSchemaConversion(Schema schema, BiConsumer<PageBuilder, RowEncoder> dataGen, Consumer<GenericRecord> validator) throws Exception
    {
        System.out.println("Schema >> " + schema);

        AvroSchemaConverter converter = new AvroSchemaConverter(new TestingTypeManager(), AvroSchemaConverter.EmptyFieldStrategy.IGNORE);
        List<Type> types = converter.convertAvroSchema(schema);
        System.out.println("Types >> " + types);

        List<EncoderColumnHandle> columns = new ArrayList<>();
        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            columns.add(new KafkaColumnHandle(
                    fields.get(i).name(),
                    types.get(i),
                    fields.get(i).name(),
                    null,
                    null,
                    false,
                    false,
                    false));
        }

        RowEncoder encoder = ENCODER_FACTORY.create(SESSION, Optional.empty(), Optional.of(schema.toString()), columns);
        PageBuilder pb = new PageBuilder(types);

        dataGen.accept(pb, encoder);

        byte[] bytes = encoder.toByteArray();
        System.out.println("Bytes >> " + bytes.length);

        File file = new File("test.bin");
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bytes);
        fos.close();

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dfReader = new DataFileReader<GenericRecord>(file, reader);
        GenericRecord record = dfReader.next();
        System.out.println("Record >> " + record);

        validator.accept(record);
    }
}
