package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * File structure :
 * Sequential tombstones, each one consists of:
 *      int deltaObjectLength
 *      byte[] deltaObjectBytes
 *      int measurementLength
 *      byte[] measurementBytes
 *      long deleteTimestamp
 */
public class LocalTombstoneAccessor implements ITombstoneAccessor {

    private static final String OPEN_MODE = "rw";
    private static final String ENCODING = "utf-8";

    /**
     * Reader and writer of tombstones.
     */
    private RandomAccessFile raf;
    /**
     * The path of raf, not necessary to be absolute.
     */
    private String filePath;

    public LocalTombstoneAccessor(String filePath) throws IOException {
        this.raf = new RandomAccessFile(filePath, OPEN_MODE);
        this.filePath = filePath;
    }

    @Override
    public List<Tombstone> readAll() throws IOException {
        List<Tombstone> tombstones = new ArrayList<>();
        raf.seek(0);
        while(raf.getFilePointer() + 4 < raf.length()) {
            int deltaObjectLength = raf.readInt();
            byte[] deltaObjectBytes = new byte[deltaObjectLength];
            raf.readFully(deltaObjectBytes);
            int measurementLength = raf.readInt();
            byte[] measurementBytes = new byte[measurementLength];
            raf.readFully(measurementBytes);
            long deleteTimestamp = raf.readLong();
            long executeTimestamp = raf.readLong();
            tombstones.add(new Tombstone(new String(deltaObjectBytes, ENCODING), new String(measurementBytes, ENCODING),
                    deleteTimestamp, executeTimestamp));
        }
        return tombstones;
    }

    @Override
    public void append(Tombstone tombstone) throws IOException {
        append(tombstone.deltaObjectId, tombstone.measurementId, tombstone.deleteTimestamp, tombstone.executeTimestamp);
    }

    @Override
    public void append(List<Tombstone> tombstones) throws IOException {
        for(Tombstone tombstone : tombstones) {
            append(tombstone);
        }
    }

    @Override
    public void append(String deltaObjectName, String measurementName, long deleteTimestamp, long executeTimestamp) throws IOException {
        raf.seek(raf.length());
        byte[] deltaObjectNameBytes = deltaObjectName.getBytes(ENCODING);
        raf.writeInt(deltaObjectNameBytes.length);
        raf.write(deltaObjectNameBytes);
        byte[] measurementNameBytes = measurementName.getBytes(ENCODING);
        raf.writeInt(measurementNameBytes.length);
        raf.write(measurementNameBytes);
        raf.writeLong(deleteTimestamp);
        raf.writeLong(executeTimestamp);
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return raf.length() == 0;
    }

    @Override
    public boolean delete() throws IOException {
        raf.close();
        return new File(this.filePath).delete();
    }
}
