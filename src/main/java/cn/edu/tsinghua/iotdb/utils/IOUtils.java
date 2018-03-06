package cn.edu.tsinghua.iotdb.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IOUtils {
    public static void writeString(OutputStream outputStream, String str, String encoding, ThreadLocal<ByteBuffer> encodingBufferLocal) throws IOException {
        byte[] strBuffer =str.getBytes(encoding);
        writeInt(outputStream, strBuffer.length, encodingBufferLocal);
        outputStream.write(strBuffer);
    }

    public static void writeInt(OutputStream outputStream, int i, ThreadLocal<ByteBuffer> encodingBufferLocal) throws IOException {
        ByteBuffer encodingBuffer;
        if(encodingBufferLocal != null) {
            encodingBuffer = encodingBufferLocal.get();
            if(encodingBuffer == null) {
                encodingBuffer = ByteBuffer.allocate(8);
                encodingBufferLocal.set(encodingBuffer);
            }
        } else {
            encodingBuffer = ByteBuffer.allocate(4);
        }
        encodingBuffer.clear();
        encodingBuffer.putInt(i);
        outputStream.write(encodingBuffer.array(), 0, Integer.BYTES);
    }

    public static String readString(ByteBuffer buffer, String encoding, ThreadLocal<byte[]> strBufferLocal) throws IOException {
        byte[] strBuffer;
        int length = buffer.getInt();
        if(strBufferLocal != null) {
            strBuffer = strBufferLocal.get();
            if(strBuffer == null || length > strBuffer.length) {
                strBuffer = new byte[length];
                strBufferLocal.set(strBuffer);
            }
        } else {
            strBuffer = new byte[length];
        }

        buffer.get(strBuffer, 0, length);
        return new String(strBuffer, 0, length, encoding);
    }
}
