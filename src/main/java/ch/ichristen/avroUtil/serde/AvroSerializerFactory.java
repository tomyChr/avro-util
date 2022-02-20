package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorType;

import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;


public class AvroSerializerFactory {

    public Serializer serializer(AvroFormat avroFormat, CompressorType compressorType) {
        if (avroFormat == AvroFormat.BINARY) {
            if (compressorType == CompressorType.NONE) {
                return new AvroBinarySerializer();
            } else if (compressorType == CompressorType.GZIP) {
                return new AvroBinarySerializer(GZIPOutputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new AvroBinarySerializer(DeflaterOutputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        } else if (avroFormat == AvroFormat.JSON) {
            if (compressorType == CompressorType.NONE) {
                return new AvroJsonSerializer();
            } else if (compressorType == CompressorType.GZIP) {
                return new AvroJsonSerializer(GZIPOutputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new AvroJsonSerializer(DeflaterOutputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        }
        throw new IllegalArgumentException("Unsupported avro format");
    }
}

