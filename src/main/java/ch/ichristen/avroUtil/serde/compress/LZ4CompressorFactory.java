package ch.ichristen.avroUtil.serde.compress;

// import net.jpountz.lz4.LZ4Factory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LZ4CompressorFactory implements CompressorFactory {
    @Override
    public FilterOutputStream getCompressor(OutputStream outputStream) throws IOException {
        // https://github.com/lz4/lz4-java
//        return LZ4Factory.fastestJavaInstance(outputStream);
        return null;
    }


//    https://stackoverflow.com/questions/36012183/java-lz4-compression-using-input-output-streams
//    public void LZ4Compress(InputStream in, OutputStream out){
//        int noBytesRead = 0;        //number of bytes read from input
//        int noBytesProcessed = 0;   //number of bytes processed
//        try {
//            while ((noBytesRead = in.read(inputBuffer)) >= 0) {
//                noBytesProcessed = inputBuffer.length;
//                decompressedLength = inputBuffer.length;
//                outputBuffer = compress(inputBuffer, decompressedLength);
//                out.write(outputBuffer);
//            }
//            out.flush();
//            in.close();
//            out.close();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//
//    public void LZ4decompress(InputStream in, OutputStream out){
//        int noBytesRead = 0;        //number of bytes read from input
//        try {
//            while((noBytesRead = in.read(inputBuffer)) >= 0){
//                noBytesProcessed = inputBuffer.length;
//                outputBuffer = decompress(inputBuffer);
//                out.write(outputBuffer, 0, noBytesRead);
//
//            }
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//
//    public static byte[] compress(byte[] src, int srcLen) {
//        decompressedLength = srcLen;
//        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
//        byte[] compressed = new byte[maxCompressedLength];
//        int compressLen = compressor.compress(src, 0, decompressedLength, compressed, 0, maxCompressedLength);
//        byte[] finalCompressedArray = Arrays.copyOf(compressed, compressLen);
//        return finalCompressedArray;
//    }
//
//    private static LZ4SafeDecompressor decompressor = factory.safeDecompressor();
//
//    public static byte[] decompress(byte[] finalCompressedArray) {
//        byte[] restored = new byte[finalCompressedArray.length];
//        restored = decompressor.decompress(finalCompressedArray, finalCompressedArray.length);
//        return restored;
//    }
}
