package ch.ichristen.avroUtil.http.converter;

import ch.ichristen.avroUtil.serde.AvroDeserializerFactory;
import ch.ichristen.avroUtil.serde.AvroFormat;
import ch.ichristen.avroUtil.serde.AvroSerializerFactory;
import ch.ichristen.avroUtil.serde.compress.CompressorType;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.http.MediaType;

public class AvroBinaryHttpMessageConverter<T extends SpecificRecord> extends AbstractAvroHttpMessageConverter<T> {

    protected static final String AVRO_BINARY_SUBTYPE = "avro";
    protected static final String AVRO_ANY_BINARY_SUBTYPE = "*+avro";
    @Deprecated
    protected static final String AVRO_BINARY_GZIP_SUBTYPE = "avro+gzip";
    @Deprecated
    protected static final String AVRO_BINARY_DEFLATE_SUBTYPE = "avro+deflate";

    public static final String AVRO_BINARY = APPLICATION + "/" + AVRO_BINARY_SUBTYPE;
    public static final String AVRO_BINARY_GZIP = APPLICATION + "/" + AVRO_BINARY_GZIP_SUBTYPE;
    public static final String AVRO_BINARY_DEFLATE = APPLICATION + "/" + AVRO_BINARY_DEFLATE_SUBTYPE;

    public AvroBinaryHttpMessageConverter(CompressorType compressorType) {
        super(  new AvroSerializerFactory<>().serializer(AvroFormat.BINARY, compressorType),
                new AvroDeserializerFactory<>().deserializer(AvroFormat.BINARY, compressorType),
                new MediaType(APPLICATION, acceptContentHeader(compressorType), DEFAULT_CHARSET));
    }

    private static String acceptContentHeader(CompressorType compressorType) {
        if (compressorType == CompressorType.NONE) {
            return AVRO_BINARY_SUBTYPE;
        } else if (compressorType == CompressorType.DEFLATER) {
            return AVRO_BINARY_DEFLATE_SUBTYPE;
        } else if (compressorType == CompressorType.GZIP) {
            return AVRO_BINARY_GZIP_SUBTYPE;
        }
        throw new IllegalArgumentException("Unsupported compressor type");
    }

}
