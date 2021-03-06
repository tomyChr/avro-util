package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorFactory;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.io.OutputStream;

public class AvroBinarySerializer extends AbstractAvroSerializer {

    public AvroBinarySerializer() {
        super();
    }

    public AvroBinarySerializer(CompressorFactory compressorFactory) {
        super(compressorFactory);
    }

    @Override
    Encoder getEncoder(GenericContainer data, OutputStream outputStream) throws IOException {
        return EncoderFactory.get().binaryEncoder(outputStream, null);
    }

}
