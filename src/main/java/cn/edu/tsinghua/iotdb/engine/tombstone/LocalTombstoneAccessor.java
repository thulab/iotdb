package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * File structure :
 * Sequential tombstones, each one consists of:
 *      int timeseriesLength
 *      byte[] timeseriesBytes
 *      long deleteTimestamp
 */
public class LocalTombstoneAccessor implements ITombstoneAccessor {

    private static final String OPEN_MODE = "rw";
    private static final String ENCODING = "utf-8";

    private RandomAccessFile raf;

    public LocalTombstoneAccessor(String filePath) throws IOException {
        raf = new RandomAccessFile(filePath, OPEN_MODE);
    }

    @Override
    public List<Tombstone> readAll() throws IOException {
        List<Tombstone> tombstones = new ArrayList<>();
        while(raf.getFilePointer() + 4 < raf.length()) {
            int nameLength = raf.readInt();
            byte[] seriesNameBytes = new byte[nameLength];
            raf.readFully(seriesNameBytes);
            long deleteTimestamp = raf.readLong();
            long executeTimestamp = raf.readLong();
            tombstones.add(new Tombstone(new String(seriesNameBytes, ENCODING), deleteTimestamp, executeTimestamp));
        }
        return tombstones;
    }

    @Override
    public void append(Tombstone tombstone) throws IOException {
        append(tombstone.seriesName, tombstone.deleteTimestamp, tombstone.executeTimestamp);
    }

    @Override
    public void append(List<Tombstone> tombstones) throws IOException {
        for(Tombstone tombstone : tombstones) {
            append(tombstone);
        }
    }

    @Override
    public void append(String seriesName, long deleteTimestamp, long executeTimestamp) throws IOException {
        raf.seek(raf.length());
        byte[] seriesNameBytes = seriesName.getBytes(ENCODING);
        raf.writeInt(seriesNameBytes.length);
        raf.write(seriesNameBytes);
        raf.writeLong(deleteTimestamp);
        raf.writeLong(executeTimestamp);
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
