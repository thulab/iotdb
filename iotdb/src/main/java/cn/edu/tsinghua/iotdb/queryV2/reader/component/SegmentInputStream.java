package cn.edu.tsinghua.iotdb.queryV2.reader.component;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * <p>
 * An implements of InputStream which can reduce the total amount of opened files in one thread.
 * </p>
 * <p>
 * IMPORTANT: Multiple SegmentInputStream with same RandomAccessFile reference could not be used in different thread.
 * </p>
 */
public class SegmentInputStream extends InputStream {
    private RandomAccessFile randomAccessFile;
    private long offset;
    private long position;
    private long size;
    private long mark;

    public SegmentInputStream() {}

    public SegmentInputStream(RandomAccessFile randomAccessFile, long offset, long size) throws IOException {
        this.randomAccessFile = randomAccessFile;
        this.offset = offset;
        this.size = size;
        this.position = offset;
        this.mark = offset;
    }


    @Override
    public int read() throws IOException {
        if (position >= offset + size) {
            return -1;
        }
        randomAccessFile.seek(position);
        int b = randomAccessFile.read();
        position += 1;
        return b;
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
        checkPosition();

        int total = randomAccessFile.read(b, offset, length);
        position += total;
        return total;
    }

    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }
        if (position + n >= offset + size) {
            long skipped = offset + size - position;
            position = offset + size;
            return skipped;
        } else {
            position += n;
            return n;
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        mark = readlimit;
    }

    @Override
    public synchronized void reset() {
        position = mark;
    }

    @Override
    public int available() {
        int left = (int) (offset + size - position);
        return left > 0 ? left : 0;
    }

    private void checkPosition() throws IOException {
        if (position >= offset + size) {
            throw new IOException("no available byte in current stream");
        }

        randomAccessFile.seek(position);
    }
}