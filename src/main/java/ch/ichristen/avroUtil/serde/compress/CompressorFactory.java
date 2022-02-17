package ch.ichristen.avroUtil.serde.compress;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@FunctionalInterface
public interface CompressorFactory {

    FilterOutputStream getCompressor(OutputStream outputStream) throws IOException;
}
