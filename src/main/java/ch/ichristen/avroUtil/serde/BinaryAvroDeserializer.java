package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.DecompressorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

@Slf4j
public class BinaryAvroDeserializer<T extends SpecificRecord> extends AbstractAvroDeserializer<T> {

    public BinaryAvroDeserializer() { super(); }

    public BinaryAvroDeserializer(DecompressorFactory decompressorFactory) {
        super(decompressorFactory);
    }

    @Override
    Collection<T> deserializeCollection(byte[] data, Class<? extends T> clazz, Schema schema, SpecificDatumReader<T> datumReader) throws IOException {
        final ArrayList<T> resultList = new ArrayList<>();

        FilterInputStream compressorInputStream = null;
        try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
            compressorInputStream = (decompressorFactory != null) ? decompressorFactory.getDecompressor(byteArrayInputStream) : null;

            final BinaryDecoder decoder = (BinaryDecoder)getDecoder(schema,(compressorInputStream != null) ? compressorInputStream : byteArrayInputStream);
            while(!decoder.isEnd()) {
                final SpecificRecord avroRecord = datumReader.read(null, decoder);
                if (log.isDebugEnabled()) {
                    log.debug("Avro object = {} : {}", clazz.getName(), avroRecord);
                }
                resultList.add((T) avroRecord);
            }
            return resultList;
        }
    }

    @Override
    Decoder getDecoder(Schema schema, InputStream inputStream) throws IOException {
        return DecoderFactory.get().binaryDecoder(inputStream, null);
    }
}
