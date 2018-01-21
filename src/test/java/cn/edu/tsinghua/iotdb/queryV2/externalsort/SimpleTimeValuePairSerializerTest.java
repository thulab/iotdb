package cn.edu.tsinghua.iotdb.queryV2.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.SimpleTimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.SimpleTimeValuePairSerializer;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class SimpleTimeValuePairSerializerTest {

    @Test
    public void test() throws IOException, ClassNotFoundException {
        String rootPath = "d1";
        String filePath = rootPath + "/d2/d3/tmpFile1";
        int count = 10000;
        testReadWrite(count, rootPath, filePath);
    }

    @Test
    public void testWithoutParentFolder() throws IOException, ClassNotFoundException {
        String rootPath = "tmpFile2";
        String filePath = rootPath;
        int count = 1000000;
        testReadWrite(count, rootPath, filePath);
    }

    private void testReadWrite(int count, String rootPath, String filePath) throws IOException, ClassNotFoundException {
        TimeValuePair[] timeValuePairs = genTimeValuePairs(count);
        SimpleTimeValuePairSerializer serializer = new SimpleTimeValuePairSerializer(filePath);
        for (TimeValuePair timeValuePair : timeValuePairs) {
            serializer.write(timeValuePair);
        }
        serializer.close();

        SimpleTimeValuePairDeserializer deserializer = new SimpleTimeValuePairDeserializer(filePath);
        int idx = 0;
        while (deserializer.hasNext()) {
            TimeValuePair timeValuePair = deserializer.next();
            Assert.assertEquals(timeValuePairs[idx].getValue(), timeValuePair.getValue());
            Assert.assertEquals(timeValuePairs[idx].getTimestamp(), timeValuePair.getTimestamp());
            idx++;
        }
        Assert.assertEquals(count, idx);
        deleteFileRecursively(new File(rootPath));
    }

    private void deleteFileRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                deleteFileRecursively(f);
            }
        }
        if (!file.delete())
            throw new IOException("Failed to delete file: " + file);
    }

    private TimeValuePair[] genTimeValuePairs(int count) {
        TimeValuePair[] timeValuePairs = new TimeValuePair[count];
        for (int i = 0; i < count; i++) {
            timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
        }
        return timeValuePairs;
    }

}
