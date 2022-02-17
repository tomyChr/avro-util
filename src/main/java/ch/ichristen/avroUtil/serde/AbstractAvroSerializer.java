package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorFactory;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

@Slf4j
public abstract class AbstractAvroSerializer<T extends SpecificRecord> implements Serializer<T> {

    private final CompressorFactory compressorFactory;

    public AbstractAvroSerializer(CompressorFactory compressorFactory) {
        this.compressorFactory = compressorFactory;
    }

    public AbstractAvroSerializer() {
        this(null);
    }

    /**
     * Implements serialization functionality from {@link SpecificRecord} type to a byte array
     *
     * @throws SerializationException wrapping any occurrence of {@link IOException} related to
     *                                    I/O operations with data and signals that some other serialization exception
     *                                    has occurred.
     */
    @Override
    public byte[] serialize(final T data) throws SerializationException {
        byte[] result = null;

        if (data != null) {
            log.debug("Avro object = {} : {}", data.getSchema().getFullName(), data);

            FilterOutputStream compressorOutputStream = null;
            try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                compressorOutputStream = (compressorFactory != null) ? compressorFactory.getCompressor(byteArrayOutputStream) : null;

                final Encoder encoder = getEncoder(data, ((compressorOutputStream != null) ? compressorOutputStream : byteArrayOutputStream));
                final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
                datumWriter.write(data, encoder);
                encoder.flush();
                if (compressorOutputStream != null) {
                    compressorOutputStream.flush();
                    compressorOutputStream.close();
                }
                byteArrayOutputStream.flush();
                result = byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                log.warn("Issue with serialization", e);
                if (compressorOutputStream != null) {
                    try {
                        compressorOutputStream.flush();
                        compressorOutputStream.close();
                    } catch (IOException ioe) {
                        // ignore
                    }
                }
                throw new SerializationException("Can't serialize the data = '" + data + "'", e);
            }
        }
        return result;
    }

    @NonNull
    public byte[] serialize(final Iterator<T> iterator) throws SerializationException {
        Encoder encoder = null;
        DatumWriter<T> datumWriter = null;
        FilterOutputStream compressorOutputStream = null;
        OutputStream outputStream = null;

        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            while (iterator.hasNext()) {
                T data = iterator.next();
                if (log.isDebugEnabled())
                    log.debug("Avro object = {} : {}", data.getSchema().getFullName(), data);
                if (encoder == null) {
                    compressorOutputStream = (compressorFactory != null) ? compressorFactory.getCompressor(byteArrayOutputStream) : null;
                    outputStream = ((compressorOutputStream != null) ? compressorOutputStream : byteArrayOutputStream);

                    // now that we have our first object we can get the schema and initialize the encoder
                    // and datumWriter
                    encoder = getEncoder(data, outputStream);
                    datumWriter = new SpecificDatumWriter<>(data.getSchema());
                    // a bit a hack but we need to add the array symbol before the first record
                    beginArray(outputStream);
                }
                datumWriter.write(data, encoder);
                encoder.flush();
                if (iterator.hasNext()) {
                    outputStream.flush();
                    nextRecord(outputStream);
                }
            }
            if (encoder != null) {
                endArray(outputStream);
                outputStream.flush();
                outputStream.close();
                log.debug("Serialized Avro object = {}", byteArrayOutputStream);
                return byteArrayOutputStream.toByteArray();
            } else {
                log.warn("Can't serialize NULL object. Returning NULL value");
                return null;
            }
        } catch (IOException e) {
            log.warn("Issue with serialization", e);
            if (compressorOutputStream != null) {
                try {
                    compressorOutputStream.flush();
                    compressorOutputStream.close();
                } catch (IOException ioe) {
                    // ignore
                }
            }
            throw new SerializationException("Can't serialize the data = '" + iterator + "'", e);
        }
    }

    void beginArray(OutputStream outputStream) throws IOException {
        // overwrite if a begin array sequence needs to be inserted into the output stream
    }

    void nextRecord(OutputStream outputStream) throws IOException {
        // overwrite in case before the next record a sequence needs to be inserted into the output stream
    }

    void endArray(OutputStream outputStream) throws IOException {
        // overwrite if an end array sequence needs to be inserted into the output stream
    }

    abstract Encoder getEncoder(T data, OutputStream outputStream) throws IOException;

}
