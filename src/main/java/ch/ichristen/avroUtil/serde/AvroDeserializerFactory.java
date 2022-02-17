package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorType;
import org.apache.avro.specific.SpecificRecord;

import java.util.zip.*;


public class AvroDeserializerFactory<T extends SpecificRecord> {

    public Deserializer<T> deserializer(AvroFormat avroFormat, CompressorType compressorType) {
        if (avroFormat == AvroFormat.BINARY) {
            if (compressorType == CompressorType.NONE) {
                return new BinaryAvroDeserializer<T>();
            } else if (compressorType == CompressorType.GZIP) {
                return new BinaryAvroDeserializer<T>(GZIPInputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new BinaryAvroDeserializer<T>(InflaterInputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        } else if (avroFormat == AvroFormat.JSON) {
            if (compressorType == CompressorType.NONE) {
                return new JsonAvroDeserializer<T>();
            } else if (compressorType == CompressorType.GZIP) {
                return new JsonAvroDeserializer<T>(GZIPInputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new JsonAvroDeserializer<T>(InflaterInputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        }
        throw new IllegalArgumentException("Unsupported avro format");
    }
}
