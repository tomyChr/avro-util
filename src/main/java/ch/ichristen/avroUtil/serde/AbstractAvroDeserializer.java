package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.DecompressorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Provides deserialization from a byte array to an Avro object
 *
 * @author Thomas Christen
 */
@Slf4j
public abstract class AbstractAvroDeserializer implements Deserializer {

    final DecompressorFactory decompressorFactory;

    public AbstractAvroDeserializer(DecompressorFactory decompressorFactory) {
        this.decompressorFactory = decompressorFactory;
    }

    public AbstractAvroDeserializer() {
        this(null);
    }



    /**
     * Implements deserialization functionality from byte array to a {@link SpecificRecord} type
     *
     * @throws SerializationException wrapping any occurrence of {@link IOException} related to
     *                                    I/O operations with data and signals that some other
     *                                    deserialization exception has occurred.
     */
    public Object deserialize(final Class clazz, final byte[] data) throws SerializationException {
        if (clazz == null) {
            throw new SerializationException("Unable to derive SpecificRecord from provided class parameter (null)");
        }
        if (clazz.isArray() && SpecificRecord.class.isAssignableFrom(clazz.componentType())) {
            ArrayList arrayList = (ArrayList) deserializeCollection(clazz.componentType(), data);
            return arrayList != null ? arrayList.toArray((SpecificRecord[])Array.newInstance(clazz.getComponentType(), arrayList.size())) : null;
        }
        if (SpecificRecord.class.isAssignableFrom(clazz)) {
            return deserializeSpecificRecord(clazz, data);
        }
        throw new SerializationException("Unsupported result class ("+ clazz + "). Currently only SpecificRecord or SpecificRecord[] is supported.");
    }

    SpecificRecord deserializeSpecificRecord(final Class<SpecificRecord> clazz, final byte[] data) throws SerializationException {
        try {
            SpecificRecord result = null;
            if (data != null) {
                if (log.isDebugEnabled()) {
                    log.debug("data = ({})", new String(data));
                }
                final Schema schema = clazz.getDeclaredConstructor().newInstance().getSchema();
                final DatumReader<SpecificRecord> datumReader = new SpecificDatumReader<>(schema);
                FilterInputStream compressorInputStream = null;
                try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
                    compressorInputStream = (decompressorFactory != null) ? decompressorFactory.getDecompressor(byteArrayInputStream) : null;
                    final Decoder decoder = getDecoder(schema, (compressorInputStream != null) ? compressorInputStream : byteArrayInputStream);
                    result = datumReader.read(null, decoder);
                    if (log.isDebugEnabled()) {
                        log.debug("Avro object = {} : {}", clazz.getName(), result);
                    }
                }
            }
            return result;
        } catch (InstantiationException | IllegalAccessException | IOException | NoSuchMethodException | InvocationTargetException e) {
            throw new SerializationException("Can't deserialize data '" + new String(data) + "'", e);
        }
    }

    public Collection<SpecificRecord> deserializeCollection(final Class<? extends SpecificRecord> clazz, final byte[] data) throws SerializationException {
        try {
            if (data != null) {
                if (log.isDebugEnabled()) {
                    log.debug("data = ({})", new String(data));
                }
                final Schema schema = clazz.getDeclaredConstructor().newInstance().getSchema();
                final SpecificDatumReader<SpecificRecord> datumReader = new SpecificDatumReader<>(schema);

                return deserializeCollection(data, clazz, schema, datumReader);
            }
            return null;
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException | NoSuchMethodException | IOException e) {
            throw new SerializationException("Can't deserialize data '" + new String(data) + "'", e);
        }
    }

    abstract ArrayList<SpecificRecord> deserializeCollection(byte[] data, Class<? extends SpecificRecord> clazz, Schema schema, SpecificDatumReader<SpecificRecord> datumReader) throws IOException;


    abstract Decoder getDecoder(Schema schema, InputStream inputStream) throws IOException;
}
