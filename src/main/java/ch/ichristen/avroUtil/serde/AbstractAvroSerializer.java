package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorFactory;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.*;
import java.util.*;

@Slf4j
public abstract class AbstractAvroSerializer implements Serializer {

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
    public byte[] serialize(final Object data) throws SerializationException {
        if (data == null) {
            return null;
        }
        if (data.getClass().isArray() && SpecificRecord.class.isAssignableFrom(data.getClass().componentType())) {
            return serializeIterator(((List)Arrays.asList((SpecificRecord[])data)).iterator());
        }
        if (SpecificRecord.class.isAssignableFrom(data.getClass())) {
            return serializeSpecificRecord((SpecificRecord)data);
        }
        if (Collection.class.isAssignableFrom(data.getClass())) {
            return serializeIterator(((Collection)data).iterator());
        }
        if (Iterator.class.isAssignableFrom(data.getClass())) {
            return serializeIterator((Iterator)data);
        }
        throw new SerializationException("Unable to serialize object with class " + data.getClass());
    }

    private byte[] serializeSpecificRecord(SpecificRecord data) throws SerializationException {
        byte[] result;
        log.debug("Avro object = {} : {}", data.getSchema().getFullName(), data);

        FilterOutputStream compressorOutputStream = null;
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            compressorOutputStream = (compressorFactory != null) ? compressorFactory.getCompressor(byteArrayOutputStream) : null;

            final Encoder encoder = getEncoder(data, ((compressorOutputStream != null) ? compressorOutputStream : byteArrayOutputStream));
            final DatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<>(data.getSchema());
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
        return result;
    }

    @NonNull
    private byte[] serializeIterator(final Iterator<SpecificRecord> iterator) throws SerializationException {
        Encoder encoder = null;
        DatumWriter datumWriter = null;
        FilterOutputStream compressorOutputStream = null;
        OutputStream outputStream = null;

        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            while (iterator.hasNext()) {
                SpecificRecord data = iterator.next();
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
                if (Closeable.class.isAssignableFrom(iterator.getClass())) {
                    try {
                        ((Closeable) iterator).close();
                    }
                    catch (IOException e) {
                        log.error("Error while closing iterator after serialization.", e);
                    }
                }
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
                    if (Closeable.class.isAssignableFrom(iterator.getClass())) ((Closeable) iterator).close();
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

    abstract Encoder getEncoder(SpecificRecord data, OutputStream outputStream) throws IOException;

}
