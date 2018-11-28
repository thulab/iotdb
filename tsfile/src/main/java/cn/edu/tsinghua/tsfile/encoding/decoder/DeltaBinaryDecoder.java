package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.encoding.encoder.DeltaBinaryEncoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is a decoder for decoding the byte array that encoded by
 * {@code DeltaBinaryEncoder}.DeltaBinaryDecoder just supports integer and long values.<br>
 *
 * @author kangrong
 * @see DeltaBinaryEncoder
 */
public abstract class DeltaBinaryDecoder extends Decoder {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaBinaryDecoder.class);
    protected long count = 0;
    protected byte[] deltaBuf;

    /**
     * the first value in one pack.
     */
    protected int readIntTotalCount = 0;
    protected int nextReadIndex = 0;
    /**
     * max bit length of all value in a pack
     */
    protected int packWidth;
    /**
     * data number in this pack
     */
    protected int packNum;

    /**
     * how many bytes data takes after encoding
     */
    protected int encodingLength;

    public DeltaBinaryDecoder() {
        super(TSEncoding.TS_2DIFF);
    }

    protected abstract void readHeader(ByteBuffer in) throws IOException;

    protected abstract void allocateDataArray();

    protected abstract void readValue(int i);

    /**
     * calculate the bytes length containing v bits
     *
     * @param v - number of bits
     * @return number of bytes
     */
    protected int ceil(int v) {
        return (int) Math.ceil((double) (v) / 8.0);
    }


    @Override
    public boolean hasNext(ByteBuffer in) throws IOException {
        return (nextReadIndex < readIntTotalCount) || in.remaining() > 0;
    }


    public static class IntDeltaDecoder extends DeltaBinaryDecoder {
        private int firstValue;
        private int[] data;
        private int previous;
        /**
         * minimum value for all difference
         */
        private int minDeltaBase;

        public IntDeltaDecoder() {
            super();
        }

        /**
         * if there's no decoded data left, decode next pack into {@code data}
         *
         * @param in InputStream
         * @return int
         * @throws IOException cannot read T from InputStream
         */
        protected int readT(ByteBuffer in) throws IOException {
            if (nextReadIndex == readIntTotalCount)
                return loadIntBatch(in);
            return data[nextReadIndex++];
        }

        @Override
        public int readInt(ByteBuffer in) {
            try {
                return readT(in);
            } catch (IOException e) {
                LOG.warn("meet IOException when load batch from InputStream, return 0");
                return 0;
            }
        }

        /**
         * if remaining data has been run out, load next pack from InputStream
         *
         * @param buffer ByteBuffer
         * @return int
         * @throws IOException cannot load batch from InputStream
         */
        protected int loadIntBatch(ByteBuffer buffer) {
            packNum = ReadWriteIOUtils.readInt(buffer);
            packWidth = ReadWriteIOUtils.readInt(buffer);
            count++;
            readHeader(buffer);

            encodingLength = ceil(packNum * packWidth);
            deltaBuf = new byte[encodingLength];
            buffer.get(deltaBuf);
            allocateDataArray();

            previous = firstValue;
            readIntTotalCount = packNum;
            nextReadIndex = 0;
            readPack();
            return firstValue;
        }

        private void readPack() {
            for (int i = 0; i < packNum; i++) {
                readValue(i);
                previous = data[i];
            }
        }

        @Override
        protected void readHeader(ByteBuffer in) {
            minDeltaBase = ReadWriteIOUtils.readInt(in);
            firstValue = ReadWriteIOUtils.readInt(in);
        }

        @Override
        protected void allocateDataArray() {
            data = new int[packNum];
        }

        @Override
        protected void readValue(int i) {
            int v = BytesUtils.bytesToInt(deltaBuf, packWidth * i, packWidth);
            data[i] = previous + minDeltaBase + v;
        }

		@Override
		public void reset() {
		}
    }

    public static class LongDeltaDecoder extends DeltaBinaryDecoder {
        private long firstValue;
        private long[] data;
        private long previous;
        /**
         * minimum value for all difference
         */
        private long minDeltaBase;

        public LongDeltaDecoder() {
            super();
        }

        /**
         * if there's no decoded data left, decode next pack into {@code data}
         *
         * @param in InputStream
         * @return long value
         * @throws IOException cannot read T from InputStream
         */
        protected long readT(ByteBuffer in) throws IOException {
            if (nextReadIndex == readIntTotalCount)
                return loadIntBatch(in);
            return data[nextReadIndex++];
        }

        /***
         * if remaining data has been run out, load next pack from InputStream
         *
         * @param buffer ByteBuffer
         * @return long value
         * @throws IOException  cannot load batch from InputStream
         */
        protected long loadIntBatch(ByteBuffer buffer) throws IOException {
            packNum = ReadWriteIOUtils.readInt(buffer);
            packWidth = ReadWriteIOUtils.readInt(buffer);
            count++;
            readHeader(buffer);

            encodingLength = ceil(packNum * packWidth);
            deltaBuf = new byte[encodingLength];
            buffer.get(deltaBuf);
            allocateDataArray();

            previous = firstValue;
            readIntTotalCount = packNum;
            nextReadIndex = 0;
            readPack();
            return firstValue;
        }


        private void readPack() throws IOException {
            for (int i = 0; i < packNum; i++) {
                readValue(i);
                previous = data[i];
            }
        }

        @Override
        public long readLong(ByteBuffer in) {
            try {
                return readT(in);
            } catch (IOException e) {
                LOG.warn("meet IOException when load batch from InputStream, return 0");
                return 0;
            }
        }

        @Override
        protected void readHeader(ByteBuffer in) {
            minDeltaBase = ReadWriteIOUtils.readLong(in);
            firstValue = ReadWriteIOUtils.readLong(in);
        }

        @Override
        protected void allocateDataArray() {
            data = new long[packNum];
        }

        @Override
        protected void readValue(int i) {
            long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
            data[i] = previous + minDeltaBase + v;
        }

		@Override
		public void reset() {
		}

    }
}
