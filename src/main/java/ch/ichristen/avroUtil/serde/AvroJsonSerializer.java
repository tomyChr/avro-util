package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorFactory;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.OutputStream;

public class AvroJsonSerializer extends AbstractAvroSerializer {


    public AvroJsonSerializer() {
        super();
    }

    public AvroJsonSerializer(CompressorFactory compressorFactory) {
        super(compressorFactory);
    }

    @Override
    Encoder getEncoder(GenericContainer data, OutputStream outputStream) throws IOException {
        return EncoderFactory.get().jsonEncoder(data.getSchema(), outputStream);
    }

    @Override
    void beginArray(OutputStream outputStream) throws IOException {
        outputStream.write('[');
    }

    @Override
    void nextRecord(OutputStream outputStream) throws IOException {
        outputStream.write(',');
    }

    @Override
    void endArray(OutputStream outputStream) throws IOException {
        outputStream.write(']');
    }

}
