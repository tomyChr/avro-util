package ch.ichristen.avroUtil.http.converter;

import ch.ichristen.avroUtil.serde.AvroDeserializerFactory;
import ch.ichristen.avroUtil.serde.AvroFormat;
import ch.ichristen.avroUtil.serde.AvroSerializerFactory;
import ch.ichristen.avroUtil.serde.compress.CompressorType;
import org.springframework.http.MediaType;

/**
 * HttpMessageConverter class for AVRO using JSON data exchange.
 *
 * <p>This class adds support reading and writing JSON SpecificRecord AVRO objects
 *
 * @author Thomas Christen
 * @param <T> the converted object type
 */
public class AvroJsonHttpMessageConverter<T extends Object> extends AbstractAvroHttpMessageConverter<T> {

    protected static final String AVRO_JSON_SUBTYPE = "avro+json";
    protected static final String AVRO_ANY_JSON_SUBTYPE = "*+avro+json";
    @Deprecated
    protected static final String AVRO_JSON_GZIP_SUBTYPE = "avro+json+gzip";
    @Deprecated
    protected static final String AVRO_JSON_DEFLATE_SUBTYPE = "avro+json+deflate";

    public static final String ACCEPT_AVRO_JSON = APPLICATION_TYPE + "/" + AVRO_JSON_SUBTYPE;
    public static final String ACCEPT_AVRO_JSON_GZIP = APPLICATION_TYPE + "/" + AVRO_JSON_GZIP_SUBTYPE;
    public static final String ACCEPT_AVRO_JSON_DEFLATE = APPLICATION_TYPE + "/" + AVRO_JSON_DEFLATE_SUBTYPE;

    public static final MediaType MEDIA_AVRO_JSON = new MediaType(APPLICATION_TYPE, acceptContentHeader(CompressorType.NONE), DEFAULT_CHARSET);
    public static final MediaType MEDIA_AVRO_JSON_GZIP = new MediaType(APPLICATION_TYPE, acceptContentHeader(CompressorType.GZIP), DEFAULT_CHARSET);
    public static final MediaType MEDIA_AVRO_JSON_DEFLATE = new MediaType(APPLICATION_TYPE, acceptContentHeader(CompressorType.DEFLATER), DEFAULT_CHARSET);

    /**
     * Create a new AvroJsonHttpMessageConverter with preconfigured serializer and deserializer for JSON data exchange.
     * Compression is disabled.
     */
    public AvroJsonHttpMessageConverter() {
        super(  new AvroSerializerFactory().serializer(AvroFormat.JSON, CompressorType.NONE),
                new AvroDeserializerFactory().deserializer(AvroFormat.JSON, CompressorType.NONE),
                MEDIA_AVRO_JSON);
    }

    /**
     * Create a new AvroJsonHttpMessageConverter with preconfigured serializer and deserializer for JSON data exchange.
     * @param compressorType the selected {@link CompressorType} type
     */
    public AvroJsonHttpMessageConverter(CompressorType compressorType) {
        super(  new AvroSerializerFactory().serializer(AvroFormat.JSON, compressorType),
                new AvroDeserializerFactory().deserializer(AvroFormat.JSON, compressorType),
                new MediaType(APPLICATION_TYPE, acceptContentHeader(compressorType), DEFAULT_CHARSET));
    }

    private static String acceptContentHeader(CompressorType compressorType) {
        if (compressorType == CompressorType.NONE) {
            return AVRO_JSON_SUBTYPE;
        } else if (compressorType == CompressorType.DEFLATER) {
            return AVRO_JSON_DEFLATE_SUBTYPE;
        } else if (compressorType == CompressorType.GZIP) {
            return AVRO_JSON_GZIP_SUBTYPE;
        }
        throw new IllegalArgumentException("Unsupported compressor type");
    }

}
