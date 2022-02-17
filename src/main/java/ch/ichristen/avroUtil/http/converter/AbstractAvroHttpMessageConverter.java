package ch.ichristen.avroUtil.http.converter;

import ch.ichristen.avroUtil.serde.Deserializer;
import ch.ichristen.avroUtil.serde.SerializationException;
import ch.ichristen.avroUtil.serde.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.compress.utils.IOUtils;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.nio.charset.Charset;

@Slf4j
public abstract class AbstractAvroHttpMessageConverter<T extends SpecificRecord> extends AbstractHttpMessageConverter<T> {

    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    protected static final String APPLICATION = "application";
    protected static final String AVRO_BINARY_SUBTYPE = "avro";
    protected static final String AVRO_ANY_BINARY_SUBTYPE = "*+avro";
    @Deprecated
    protected static final String AVRO_BINARY_GZIP_SUBTYPE = "avro+gzip";
    @Deprecated
    protected static final String AVRO_BINARY_DEFLATE_SUBTYPE = "avro+deflate";

    public static final String AVRO_BINARY = APPLICATION + "/" + AVRO_BINARY_SUBTYPE;

    private Serializer<SpecificRecord> serializer;
    private Deserializer<SpecificRecord> deserializer;

    public AbstractAvroHttpMessageConverter(Serializer<SpecificRecord> serializer, Deserializer<SpecificRecord> deserializer, MediaType... supportedMediaTypes) {
        super(supportedMediaTypes);
        this.serializer = serializer;
        this.deserializer = deserializer;
    }


    /**
     * Indicates whether the given class is supported by this converter.
     * @param clazz the class to test for support
     * @return {@code true} if supported; {@code false} otherwise
     */
    @Override
    protected boolean supports(Class<?> clazz) {
        return true;
    }

    /**
     * Reads the actual object. Invoked from {@link #read}.
     * @param clazz the type of object to return
     * @param inputMessage the HTTP input message to read from
     * @return the converted object
     * @throws IOException in case of I/O errors
     * @throws HttpMessageNotReadableException in case of conversion errors
     */
    @Override
    protected T readInternal(Class<? extends T> clazz, HttpInputMessage inputMessage)
            throws IOException, HttpMessageNotReadableException {
        T result = null;
        byte[] data = IOUtils.toByteArray(inputMessage.getBody());
        if (data.length > 0) {
            try {
                result = (T) deserializer.deserialize((Class<? extends T>) clazz, data);
            } catch (SerializationException e) {
                log.error(e.getMessage(), e);
            }
        }
        return result;
    }

    /**
     * Writes the actual body. Invoked from {@link #write}.
     * @param t the object to write to the output message
     * @param outputMessage the HTTP output message to write to
     * @throws IOException in case of I/O errors
     * @throws HttpMessageNotWritableException in case of conversion errors
     */
    @Override
    protected void writeInternal(T t, HttpOutputMessage outputMessage)
            throws IOException, HttpMessageNotWritableException {
        byte[] data = new byte[0];
        try {
            data = serializer.serialize(t);
        } catch (SerializationException e) {
            log.error(e.getMessage(), e);
        }
        outputMessage.getBody().write(data);
    }




}
