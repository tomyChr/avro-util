package ch.ichristen.avroUtil.serde.compress;

import java.io.*;

@FunctionalInterface
public interface DecompressorFactory {

    FilterInputStream getDecompressor(InputStream inputStream) throws IOException;

}
