package com.nokia;


import example.avro.MM;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SimpleAvroSchemaFlink implements DeserializationSchema<MM>, SerializationSchema<MM> {


    @Override
    public byte[] serialize(MM userBehavior) {
        SpecificDatumWriter<MM> writer = new SpecificDatumWriter<MM>(userBehavior.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            writer.write(userBehavior, encoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toByteArray();
    }

    @Override
    public TypeInformation<MM> getProducedType() {
      return TypeInformation.of(MM.class);
    }

    @Override
    public MM deserialize(byte[] bytes) throws IOException {
        MM userBehavior = new MM();
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(bytes);
        SpecificDatumReader<MM> stockSpecificDatumReader = new SpecificDatumReader<MM>(userBehavior.getSchema());
        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(arrayInputStream, null);
        try {
            userBehavior=stockSpecificDatumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userBehavior;
    }

    @Override
    public boolean isEndOfStream(MM userBehavior) {
        return false;
    }
}
