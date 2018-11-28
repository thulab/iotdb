package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public abstract class GorillaDecoder extends Decoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(GorillaDecoder.class);
    protected static final int EOF = -1;
    // flag to indicate whether the first value is read from stream
    protected boolean flag;
    protected int leadingZeroNum, tailingZeroNum;
    protected boolean isEnd;
    // 8-bit buffer of bits to write out
    protected int buffer;
    // number of bits remaining in buffer
    protected int numberLeftInBuffer;


    protected boolean nextFlag1;
    protected boolean nextFlag2;


    public GorillaDecoder() {
		super(TSEncoding.GORILLA);
		reset();
	}

	public void reset() {
		this.flag = false;
		this.isEnd = false;
		this.numberLeftInBuffer = 0;
	}

    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        if (buffer.remaining() > 0 || !isEnd) {
            return true;
        }
        return false;
    }

    protected boolean isEmpty() {
        return buffer == EOF;
    }

    protected boolean readBit(ByteBuffer buffer) throws IOException {
        if (numberLeftInBuffer == 0 && !isEnd) {
            fillBuffer(buffer);
        }
        if (isEmpty())
            throw new IOException("Reading from empty input stream");
        numberLeftInBuffer--;
        return ((this.buffer >> numberLeftInBuffer) & 1) == 1;
    }

    /**
     * read one byte and save in buffer
     *
     * @param buffer ByteBuffer to read
     */
    protected void fillBuffer(ByteBuffer buffer) {
        if(buffer.remaining() >= 1) {
            this.buffer = ReadWriteIOUtils.read(buffer);
            numberLeftInBuffer = 8;
        } else {
            this.buffer = EOF;
        }

    }

    /**
     * read some bits and convert them to a int value
     *
     * @param in  stream to read
     * @param len number of bit to read
     * @return converted int value
     * @throws IOException cannot read from stream
     */
    protected int readIntFromStream(ByteBuffer in, int len) throws IOException {
        int num = 0;
        for (int i = 0; i < len; i++) {
            int bit = readBit(in) ? 1 : 0;
            num |= bit << (len - 1 - i);
        }
        return num;
    }

    /**
     * read some bits and convert them to a long value
     *
     * @param in  stream to read
     * @param len number of bit to read
     * @return converted long value
     * @throws IOException cannot read from stream
     */
    protected long readLongFromStream(ByteBuffer in, int len) throws IOException {
        long num = 0;
        for (int i = 0; i < len; i++) {
            long bit = (long) (readBit(in) ? 1 : 0);
            num |= bit << (len - 1 - i);
        }
        return num;
    }
}
