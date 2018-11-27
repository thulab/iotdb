package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.exception.TSFileDecodingException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteForEncodingUtils;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.encoding.encoder.FloatEncoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decoder for float or double value using rle or two diff. For
 * more info about encoding pattern, see {@link FloatEncoder}
 */
public class FloatDecoder extends Decoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(FloatDecoder.class);
    private Decoder decoder;

    /**
     * maxPointValue = 10^(maxPointNumer) maxPointNumber can be read from stream
     */
    private double maxPointValue;

    /**
     * flag to indicate whether we have read maxPointNumber and calculate
     * maxPointValue
     */
    private boolean isMaxPointNumberRead;

    public FloatDecoder(TSEncoding encodingType, TSDataType dataType) {
        super(encodingType);
        if (encodingType == TSEncoding.RLE) {
            if (dataType == TSDataType.FLOAT) {
                decoder = new IntRleDecoder(EndianType.LITTLE_ENDIAN);
                LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using int-rle and float");
            } else if (dataType == TSDataType.DOUBLE) {
                decoder = new LongRleDecoder(EndianType.LITTLE_ENDIAN);
                LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using long-rle and double");
            } else {
                throw new TSFileDecodingException(
                        String.format("data type %s is not supported by FloatDecoder", dataType));
            }
        } else if (encodingType == TSEncoding.TS_2DIFF) {
            if (dataType == TSDataType.FLOAT) {
                decoder = new DeltaBinaryDecoder.IntDeltaDecoder();
                LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using int-delta and float");
            } else if (dataType == TSDataType.DOUBLE) {
                decoder = new DeltaBinaryDecoder.LongDeltaDecoder();
                LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using long-delta and double");
            } else {
                throw new TSFileDecodingException(
                        String.format("data type %s is not supported by FloatDecoder", dataType));
            }
        } else {
            throw new TSFileDecodingException(
                    String.format("%s encoding is not supported by FloatDecoder", encodingType));
        }
        isMaxPointNumberRead = false;
    }

    @Override
    public float readFloat(ByteBuffer in) {
        readMaxPointValue(in);
        int value = decoder.readInt(in);
        double result = value / maxPointValue;
        return (float) result;
    }

    @Override
    public double readDouble(ByteBuffer in) {
        readMaxPointValue(in);
        long value = decoder.readLong(in);
        double result = value / maxPointValue;
        return result;
    }

    private void readMaxPointValue(ByteBuffer in) {
        try {
            if (!isMaxPointNumberRead) {
                int maxPointNumber = ReadWriteForEncodingUtils.readUnsignedVarInt(in);
                if (maxPointNumber <= 0) {
                    maxPointValue = 1;
                } else {
                    maxPointValue = Math.pow(10, maxPointNumber);
                }
                isMaxPointNumberRead = true;
            }
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding FloatDecoder: error occurs when reading maxPointValue", e);
        }
    }

    @Override
    public boolean hasNext(ByteBuffer in) throws IOException {
        if (decoder == null) {
            return false;
        }
        return decoder.hasNext(in);
    }

    @Override
    public Binary readBinary(ByteBuffer in) {
        throw new TSFileDecodingException("Method readBinary is not supproted by FloatDecoder");
    }

    @Override
    public boolean readBoolean(ByteBuffer in) {
        throw new TSFileDecodingException("Method readBoolean is not supproted by FloatDecoder");
    }

    @Override
    public short readShort(ByteBuffer in) {
        throw new TSFileDecodingException("Method readShort is not supproted by FloatDecoder");
    }

    @Override
    public int readInt(ByteBuffer in) {
        throw new TSFileDecodingException("Method readInt is not supproted by FloatDecoder");
    }

    @Override
    public long readLong(ByteBuffer in) {
        throw new TSFileDecodingException("Method readLong is not supproted by FloatDecoder");
    }
}
