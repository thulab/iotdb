package cn.edu.tsinghua.iotdb.queryV2.component;

import cn.edu.tsinghua.iotdb.queryV2.SimpleFileWriter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.SegmentInputStream;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.SegmentInputStreamWithMMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhangjinrui on 2018/1/15.
 */
public class SegmentInputStreamTest {
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
    public void testNoMMap() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(PATH, "r");
        //testOneSegmentNoMMap(randomAccessFile, 0, 1000);
        testOneSegmentNoMMap(randomAccessFile, 20, 1000);
        testOneSegmentNoMMap(randomAccessFile, 30, 1000);
        testOneSegmentNoMMap(randomAccessFile, 1000, 1000);

        randomAccessFile.close();
    }


    private void testOneSegmentNoMMap(RandomAccessFile randomAccessFile, int offset, int size) throws IOException {
        SegmentInputStream segmentInputStream = new SegmentInputStream(randomAccessFile, offset, size);
        int b;
        int index = offset;
        while ((b = segmentInputStream.read()) != -1) {
            //System.out.println(bytes[index] + " " + (byte) b);
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
            //System.out.println(bytes[i + offset]);
            Assert.assertEquals(bytes[i + offset], ret[i - startPos]);
        }
    }
}












