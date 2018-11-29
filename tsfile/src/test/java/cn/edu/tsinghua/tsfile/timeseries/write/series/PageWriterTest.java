package cn.edu.tsinghua.tsfile.timeseries.write.series;

import cn.edu.tsinghua.tsfile.common.utils.Binary;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteForEncodingUtils;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.encoding.decoder.PlainDecoder;
import cn.edu.tsinghua.tsfile.encoding.encoder.PlainEncoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.constant.TimeseriesTestConstant;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class PageWriterTest {

    @Test
    public void testNoFreq() {
        PageWriter writer = new PageWriter();
        writer.setTimeEncoder(new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.INT64, 0));
        writer.setValueEncoder(new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.INT64, 0));
        short s1 = 12;
        boolean b1 = false;
        int i1 = 1;
        long l1 = 123142120391L;
        float f1 = 2.2f;
        double d1 = 1294283.4323d;
        String str1 = "I have a dream";
        int timeCount = 0;
        try {
            writer.write(timeCount++, s1);
            writer.write(timeCount++, b1);
            writer.write(timeCount++, i1);
            writer.write(timeCount++, l1);
            writer.write(timeCount++, f1);
            writer.write(timeCount++, d1);
            writer.write(timeCount++, new Binary(str1));
            assertEquals(101, writer.estimateMaxMemSize());
            ByteBuffer buffer1 = writer.getUncompressedBytes();
            ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
            writer.reset();
            assertEquals(0, writer.estimateMaxMemSize());
            int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
            byte[] timeBytes = new byte[timeSize];
            buffer.get(timeBytes);
            ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
            PlainDecoder decoder = new PlainDecoder(EndianType.LITTLE_ENDIAN);
            for (int i = 0; i < timeCount; i++) {
                assertEquals(i, decoder.readLong(buffer2));
            }
            assertEquals(s1, decoder.readShort(buffer));
            assertEquals(b1, decoder.readBoolean(buffer));
            assertEquals(i1, decoder.readInt(buffer));
            assertEquals(l1, decoder.readLong(buffer));
            Assert.assertEquals(f1, decoder.readFloat(buffer), TimeseriesTestConstant.float_min_delta);
            assertEquals(d1, decoder.readDouble(buffer), TimeseriesTestConstant.double_min_delta);
            assertEquals(str1, decoder.readBinary(buffer).getStringValue());

        } catch (IOException e) {
            fail();
        }
    }
}
