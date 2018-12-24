package cn.edu.tsinghua.iotdb.queryV2.component;

import cn.edu.tsinghua.iotdb.queryV2.SimpleFileWriter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.SegmentInputStreamWithMMap;
import sun.misc.Cleaner;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class SegmentInputStreamWithMMapTest {


    private static final String PATH = "fileStreamManagerTestFile";
    private static int count = 10000;
    private static byte[] bytes;

    @Before
    public void before() throws IOException {
        bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = (byte) ((i % 254) + 1);
        }
        SimpleFileWriter.writeFile(PATH, bytes);
    }

    @After
    public void after() {
        File file = new File(PATH);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testWithMMap() throws Exception {
        RandomAccessFile randomAccessFile = new RandomAccessFile(PATH, "r");

        MappedByteBuffer buffer1 = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length());
        testOneSegmentWithMMap(buffer1, 0, 1000);
        testOneSegmentWithMMap(buffer1, 20, 1000);
        testOneSegmentWithMMap(buffer1, 30, 1000);
        testOneSegmentWithMMap(buffer1, 1000, 1000);
        freeDBJDK8or9(buffer1);    
//        ((DirectBuffer) buffer1).cleaner().clean();
        randomAccessFile.close();
    }

    
    static final Method cleanMethod;
    
    static {
        try {
            Class<?> cleanerOrCleanable;
            try {
                cleanerOrCleanable = Class.forName("sun.misc.Cleaner");
            } catch (ClassNotFoundException e1) {
                try {
                    cleanerOrCleanable = Class.forName("java.lang.ref.Cleaner$Cleanable");
                } catch (ClassNotFoundException e2) {
                    e2.addSuppressed(e1);
                    throw e2;
                }
           }
            cleanMethod = cleanerOrCleanable.getDeclaredMethod("clean");
       } catch (Exception e) {
            throw new Error(e);
       }
    }

    public static void freeDBJDK8or9(ByteBuffer buffer) throws Exception {
        if (buffer instanceof sun.nio.ch.DirectBuffer) {
            final Cleaner bufferCleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();

            if (bufferCleaner != null) {
                cleanMethod.invoke(bufferCleaner);
            }
        }
    }
    
    private void testOneSegmentWithMMap(MappedByteBuffer buffer, int offset, int size) throws IOException {
        SegmentInputStreamWithMMap segmentInputStream = new SegmentInputStreamWithMMap(buffer, offset, size);
        int b;
        int index = offset;
        while ((b = segmentInputStream.read()) != -1) {
            Assert.assertEquals(bytes[index], (byte) b);
            index++;
        }
        Assert.assertEquals(index, size + offset);

        segmentInputStream.reset();
        int startPos = 100;
        int len = 300;
        byte[] ret = new byte[len];
        segmentInputStream.skip(startPos);
        segmentInputStream.read(ret);
        for (int i = startPos; i < len; i++) {
            Assert.assertEquals(bytes[i + offset], ret[i - startPos]);
        }
    }
}