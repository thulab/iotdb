package cn.edu.tsinghua.iotdb.queryV2.component;

import cn.edu.tsinghua.iotdb.queryV2.SimpleFileWriter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.SegmentInputStreamWithMMap;
import sun.nio.ch.DirectBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.Buffer;
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
    	String[] javaVersionElements = System.getProperty("java.version").split("\\.");
    	int major = Integer.parseInt(javaVersionElements[1]);
    	if (major == 8) {
    		((DirectBuffer) buffer1).cleaner().clean();
    	} else {
    		destroyBuffer(buffer1);
    	}
    	 randomAccessFile.close();
    }

    private void destroyBuffer(Buffer byteBuffer) throws Exception {
    	try {
        	final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        	final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
        	theUnsafeField.setAccessible(true);
        	final Object theUnsafe = theUnsafeField.get(null);
        	final Method invokeCleanerMethod = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        	invokeCleanerMethod.invoke(theUnsafe, byteBuffer);
		} catch (Exception e) {
			throw e;
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