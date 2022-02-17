package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.DecompressorFactory;
import ch.ichristen.avroUtil.serde.depricated.AvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;

@Slf4j
public class JsonAvroDeserializer<T extends SpecificRecord> extends AbstractAvroDeserializer<T> {

    protected enum ParserStatus { NA, ARRAY, RECORD };

    public JsonAvroDeserializer() { super(); }

    public JsonAvroDeserializer(DecompressorFactory decompressorFactory) {
        super(decompressorFactory);
    }

    Collection<T> deserializeCollectionOld(byte[] data, Class<? extends T> clazz, Schema schema, SpecificDatumReader<T> datumReader) throws IOException {
        final ArrayList<T> resultList = new ArrayList<>();
        int i = 0;
        int startRecord = 0;
        int openCount = 0;
//////////////////////////////////////////////////////////////////////
//        O L D  V E R S I O N  !!!!!!!!!!!!
//////////////////////////////////////////////////////////////////////
        ParserStatus parserStatus = ParserStatus.NA;
        while (i < data.length) {
            if (parserStatus == ParserStatus.NA) {
                if (data[i] == '[') {
                    parserStatus = ParserStatus.ARRAY;
                }
            } else if (parserStatus == ParserStatus.ARRAY) {
                if (data[i] == '{') {
                    parserStatus = ParserStatus.RECORD;
                    openCount = 1;
                    startRecord = i;
                    // } else if (data[i] == ',') {
                    // ignore
                } else if (data[i] == ']') {
                    parserStatus = ParserStatus.NA;
                }
            } else {  // parserStatus == ParserStatus.RECORD
                if (data[i] == '}') {
                    openCount--;
                    if (openCount == 0) {
                        // now carve out the part start - i+1 and use a datumReader to create avro object
                        try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, startRecord, i - startRecord + 1)) {
                            if (log.isDebugEnabled()) {
                                byteArrayInputStream.mark(0);
                                log.debug("Record for parser '{}'", new String(byteArrayInputStream.readAllBytes()));
                                byteArrayInputStream.reset();
                            }
                            final Decoder decoder = getDecoder(schema, byteArrayInputStream);
                            final SpecificRecord avroRecord = datumReader.read(null, decoder);
                            if (log.isDebugEnabled()) {
                                log.debug("Avro object = {} : {}", clazz.getName(), avroRecord);
                            }
                            // add it to the collection
                            resultList.add((T) avroRecord);
                        }
                        parserStatus = ParserStatus.ARRAY;
                    }
                } else if (data[i] == '{') {
                    openCount++;
                }
            }
            i++;
        }
        if (parserStatus != ParserStatus.NA) {
            log.warn("Malformed json input '{}'", new String(data));
        }
        return resultList;
    }

    Collection<T> deserializeCollection(byte[] data, Class<? extends T> clazz, Schema schema, SpecificDatumReader<T> datumReader) throws IOException {
        final ArrayList<T> resultList = new ArrayList<>();
        FilterInputStream compressorInputStream = null;
        InputStream inputStream;

        int openCount = 0;
        ParserStatus parserStatus = ParserStatus.NA;

        try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
            compressorInputStream = (decompressorFactory != null) ? decompressorFactory.getDecompressor(byteArrayInputStream) : null;
            inputStream = (compressorInputStream != null) ? compressorInputStream : byteArrayInputStream;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte ch;
            while (inputStream.available()>0) {
                ch = (byte)inputStream.read();
                if (parserStatus == ParserStatus.NA) {
                    if (ch == '[') {
                        parserStatus = ParserStatus.ARRAY;
                    }
                } else if (parserStatus == ParserStatus.ARRAY) {
                    if (ch == '{') {
                        parserStatus = ParserStatus.RECORD;
                        openCount = 1;
                        // } else if (data[i] == ',') {
                        // ignore
                    } else if (ch == ']') {
                        parserStatus = ParserStatus.NA;
                    }
                } else {  // parserStatus == ParserStatus.RECORD
                    if (ch == '}') {
                        openCount--;
                        if (openCount == 0) {
                            // write last ch to the outputStream
                            byteArrayOutputStream.write(ch);
                            try (final ByteArrayInputStream oneRecordInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray())) {
                                log.debug("Record for parser '{}'", byteArrayOutputStream);
                                final Decoder decoder = getDecoder(schema, oneRecordInputStream);
                                final SpecificRecord avroRecord = datumReader.read(null, decoder);
                                if (log.isDebugEnabled()) {
                                    log.debug("Avro object = {} : {}", clazz.getName(), avroRecord);
                                }
                                // add it to the collection
                                resultList.add((T) avroRecord);
                            }
                            // reset the outputStream to be ready for the next record
                            byteArrayOutputStream.reset();
                            parserStatus = ParserStatus.ARRAY;
                        }
                    } else if (ch == '{') {
                        openCount++;
                    }
                }
                if (parserStatus == ParserStatus.RECORD) {
                    byteArrayOutputStream.write(ch);
                }
            }
        }
        if (parserStatus != ParserStatus.NA) {
            log.warn("Malformed json input '{}'", new String(data));
        }
        return resultList;
    }

    @Override
    Decoder getDecoder(Schema schema, InputStream inputStream) throws IOException {
        return DecoderFactory.get().jsonDecoder(schema, inputStream);
    }
}
