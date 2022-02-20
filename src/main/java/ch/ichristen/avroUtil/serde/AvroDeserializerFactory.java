package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorType;
import org.apache.avro.specific.SpecificRecord;

import java.util.zip.*;


public class AvroDeserializerFactory {

    public Deserializer deserializer(AvroFormat avroFormat, CompressorType compressorType) {
        if (avroFormat == AvroFormat.BINARY) {
            if (compressorType == CompressorType.NONE) {
                return new BinaryAvroDeserializer();
            } else if (compressorType == CompressorType.GZIP) {
                return new BinaryAvroDeserializer(GZIPInputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new BinaryAvroDeserializer(InflaterInputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        } else if (avroFormat == AvroFormat.JSON) {
            if (compressorType == CompressorType.NONE) {
                return new JsonAvroDeserializer();
            } else if (compressorType == CompressorType.GZIP) {
                return new JsonAvroDeserializer(GZIPInputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new JsonAvroDeserializer(InflaterInputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        }
        throw new IllegalArgumentException("Unsupported avro format");
    }
}
