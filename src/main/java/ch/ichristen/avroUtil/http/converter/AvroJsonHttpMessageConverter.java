package ch.ichristen.avroUtil.http.converter;

import ch.ichristen.avroUtil.serde.AvroDeserializerFactory;
import ch.ichristen.avroUtil.serde.AvroFormat;
import ch.ichristen.avroUtil.serde.AvroSerializerFactory;
import ch.ichristen.avroUtil.serde.compress.CompressorType;
import org.apache.avro.specific.SpecificRecord;
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

    public static final String AVRO_JSON = APPLICATION + "/" + AVRO_JSON_SUBTYPE;
    public static final String AVRO_JSON_GZIP = APPLICATION + "/" + AVRO_JSON_GZIP_SUBTYPE;
    public static final String AVRO_JSON_DEFLATE = APPLICATION + "/" + AVRO_JSON_DEFLATE_SUBTYPE;


    /**
     * Create a new AvroJsonHttpMessageConverter with preconfigured serializer and deserializer for JSON data exchange.
     * Compression is disabled.
     */
    public AvroJsonHttpMessageConverter() {
        super(  new AvroSerializerFactory().serializer(AvroFormat.JSON, CompressorType.NONE),
                new AvroDeserializerFactory().deserializer(AvroFormat.JSON, CompressorType.NONE),
                new MediaType(APPLICATION, acceptContentHeader(CompressorType.NONE), DEFAULT_CHARSET));
    }

    /**
     * Create a new AvroJsonHttpMessageConverter with preconfigured serializer and deserializer for JSON data exchange.
     * @param compressorType the selected {@link CompressorType} type
     */
    public AvroJsonHttpMessageConverter(CompressorType compressorType) {
        super(  new AvroSerializerFactory().serializer(AvroFormat.JSON, compressorType),
                new AvroDeserializerFactory().deserializer(AvroFormat.JSON, compressorType),
                new MediaType(APPLICATION, acceptContentHeader(compressorType), DEFAULT_CHARSET));
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
