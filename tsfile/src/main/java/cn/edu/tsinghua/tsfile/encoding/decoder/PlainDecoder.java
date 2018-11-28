package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.exception.TSFileDecodingException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * @author Zhang Jinrui
 */
public class PlainDecoder extends Decoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlainDecoder.class);
    public EndianType endianType;

    public PlainDecoder(EndianType endianType) {
        super(TSEncoding.PLAIN);
        this.endianType = endianType;
    }

    @Override
    public boolean readBoolean(ByteBuffer in) {
        int ch1 = in.get();
        if (ch1 == 0) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public short readShort(ByteBuffer in) {
        int ch1 = in.get();
        int ch2 = in.get();
        if (this.endianType == EndianType.LITTLE_ENDIAN) {
            return (short) ((ch2 << 8) + ch1);
        } else {
            LOGGER.error(
                    "tsfile-encoding PlainEncoder: current version does not support short value decoding");
        }
        return -1;
    }

    @Override
    public int readInt(ByteBuffer in) {
        int ch1 = ReadWriteIOUtils.read(in);
        int ch2 = ReadWriteIOUtils.read(in);
        int ch3 = ReadWriteIOUtils.read(in);
        int ch4 = ReadWriteIOUtils.read(in);
        if (this.endianType == EndianType.LITTLE_ENDIAN) {
            return ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
        } else {
            LOGGER.error(
                    "tsfile-encoding PlainEncoder: current version does not support int value encoding");
        }
        return -1;
    }

    @Override
    public long readLong(ByteBuffer in) {
        int[] buf = new int[8];
        for (int i = 0; i < 8; i++)
            buf[i] = ReadWriteIOUtils.read(in);

        Long res = 0L;
        for (int i = 0; i < 8; i++) {
            res += ((long) buf[i] << (i * 8));
        }
        return res;
    }

    @Override
    public float readFloat(ByteBuffer in) {
        return Float.intBitsToFloat(readInt(in));
    }

    @Override
    public double readDouble(ByteBuffer in) {
        return Double.longBitsToDouble(readLong(in));
    }

    @Override
    public Binary readBinary(ByteBuffer in) {
        int length = readInt(in);
        byte[] buf = new byte[length];
        in.get(buf, 0, buf.length);
        return new Binary(buf);
    }

    @Override
    public boolean hasNext(ByteBuffer in) throws IOException {
        return in.remaining() > 0;
    }

    @Override
    public BigDecimal readBigDecimal(ByteBuffer in) {
        throw new TSFileDecodingException("Method readBigDecimal is not supproted by PlainDecoder");
    }

	@Override
	public void reset() {
	}
}
