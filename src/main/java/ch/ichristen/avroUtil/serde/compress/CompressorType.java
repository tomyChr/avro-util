package ch.ichristen.avroUtil.serde.compress;

public enum CompressorType {
     NONE       // no compression
    ,GZIP       // compression using GZIP
    ,DEFLATER   // compression using Java Deflater/Inflater
//    ,LZ4      // currently not implemented
}
