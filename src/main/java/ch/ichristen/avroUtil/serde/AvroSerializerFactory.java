package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorType;
import org.apache.avro.specific.SpecificRecord;

import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;


public class AvroSerializerFactory<T extends SpecificRecord> {

    public Serializer<T> serializer(AvroFormat avroFormat, CompressorType compressorType) {
        if (avroFormat == AvroFormat.BINARY) {
            if (compressorType == CompressorType.NONE) {
                return new AvroBinarySerializer<T>();
            } else if (compressorType == CompressorType.GZIP) {
                return new AvroBinarySerializer<T>(GZIPOutputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new AvroBinarySerializer<T>(DeflaterOutputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        } else if (avroFormat == AvroFormat.JSON) {
            if (compressorType == CompressorType.NONE) {
                return new AvroJsonSerializer<T>();
            } else if (compressorType == CompressorType.GZIP) {
                return new AvroJsonSerializer<T>(GZIPOutputStream::new);
            } else if (compressorType == CompressorType.DEFLATER) {
                return new AvroJsonSerializer<T>(DeflaterOutputStream::new);
            } else {
                throw new IllegalArgumentException("Unsupported compressor type");
            }
        }
        throw new IllegalArgumentException("Unsupported avro format");
    }
}

