package ch.ichristen.avroUtil.http.converter;

import ch.ichristen.avroUtil.serde.Deserializer;
import ch.ichristen.avroUtil.serde.SerializationException;
import ch.ichristen.avroUtil.serde.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


/**
 * Abstract class for AVRO environments.
 *
 * <p>This class adds support reading and writing SpecificRecord AVRO objects
 *
 * @author Thomas Christen
 * @param <T> the converted object type
 */
@Slf4j
public abstract class AbstractAvroHttpMessageConverter<T extends Object> extends AbstractHttpMessageConverter<T> {

    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public static final String APPLICATION_TYPE = "application";

    private final Serializer serializer;
    private final Deserializer deserializer;

    /**
     * Create a new AbstractAvroHttpMessageConverter.
     * @param serializer the configured serializer
     * @param deserializer the configured deserializer
     * @param supportedMediaTypes the list of supported media types
     */
    public AbstractAvroHttpMessageConverter(Serializer serializer, Deserializer deserializer, MediaType... supportedMediaTypes) {
        super(supportedMediaTypes);
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    /**
     * This implementation checks if the given class is supported, and if the supported media types include the given media type.
     * @param clazz - the class to test for writability
     * @param mediaType - the media type to write (can be null if not specified); typically the value of an Accept header.
     * @return true if writable; false otherwise
     */
    public boolean canWrite(Class<?> clazz, @Nullable MediaType mediaType) {
        return supportsWriting(clazz) && this.canWrite(mediaType);
    }

    protected boolean supportsWriting(Class<?> clazz) {
        if (clazz.isArray()) {
            return SpecificRecord.class.isAssignableFrom(clazz.getComponentType());
        } else {
            return SpecificRecord.class.isAssignableFrom(clazz) ||
                   Collection.class.isAssignableFrom(clazz) ||
                   Iterator.class.isAssignableFrom(clazz) ;
        }
    }

    /**
     * Indicates whether the given class is supported by this converter.
     *
     * <p>For a SpecificRecord the class needs to be known. Therefore, only single objects
     * and array of objects are currently supported (e.g. a collection.class would not
     * allow to derive the according SpecificRecord. For such cases, the serializer - and
     * deserializer class both also support Collections through an additional parameter.
     * Such use would need to be coded manually.
     *
     * @param clazz the class to test for support
     * @return {@code true} if supported; {@code false} otherwise
     */
    @Override
    protected boolean supports(Class<?> clazz) {
        if (clazz.isArray()) {
            return SpecificRecord.class.isAssignableFrom(clazz.getComponentType());
        } else {
            return SpecificRecord.class.isAssignableFrom(clazz);
        }
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
                throw new HttpMessageNotReadableException(e.getMessage(), e, inputMessage);
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
            throw new HttpMessageNotWritableException(e.getMessage(), e);
        }
        outputMessage.getBody().write(data);
    }

}
