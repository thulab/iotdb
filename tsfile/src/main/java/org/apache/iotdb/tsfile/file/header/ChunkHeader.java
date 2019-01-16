/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.file.header;

import org.apache.iotdb.tsfile.utils.ReadWriteIoUtils;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChunkHeader {
    public static final byte MARKER = MetaMarker.ChunkHeader;

    private String measurementID;
    private int dataSize;
    private TSDataType dataType;
    private CompressionType compressionType;
    private TSEncoding encodingType;
    private int numOfPages;
    /**
     * The maximum time of the tombstones that take effect on this chunk. Only data with larger timestamps than this
     * should be exposed to user.
     */
    private long maxTombstoneTime;

    /**
     * The time when the ChunkGroup of this chunk is closed. This will not be written out and will only be set when read
     * together with its ChunkGroup during querying.
     */
    private long writtenTime;// not serialized now.

    // this field does not need to be serialized.
    private int serializedSize;

    public int getSerializedSize() {
        return serializedSize;
    }

    public ChunkHeader(String measurementID, int dataSize, TSDataType dataType, CompressionType compressionType,
            TSEncoding encoding, int numOfPages) {
        this(measurementID, dataSize, dataType, compressionType, encoding, numOfPages, 0);
    }

    private ChunkHeader(String measurementID, int dataSize, TSDataType dataType, CompressionType compressionType,
            TSEncoding encoding, int numOfPages, long maxTombstoneTime) {
        this.measurementID = measurementID;
        this.dataSize = dataSize;
        this.dataType = dataType;
        this.compressionType = compressionType;
        this.numOfPages = numOfPages;
        this.encodingType = encoding;
        this.serializedSize = getSerializedSize(measurementID);
        this.maxTombstoneTime = maxTombstoneTime;
    }

    public String getMeasurementID() {
        return measurementID;
    }

    public int getDataSize() {
        return dataSize;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int length = 0;
        length += ReadWriteIoUtils.write(MetaMarker.ChunkHeader, outputStream);
        length += ReadWriteIoUtils.write(measurementID, outputStream);
        length += ReadWriteIoUtils.write(dataSize, outputStream);
        length += ReadWriteIoUtils.write(dataType, outputStream);
        length += ReadWriteIoUtils.write(numOfPages, outputStream);
        length += ReadWriteIoUtils.write(compressionType, outputStream);
        length += ReadWriteIoUtils.write(encodingType, outputStream);
        length += ReadWriteIoUtils.write(maxTombstoneTime, outputStream);
        assert length == getSerializedSize();
        return length;
    }

    public int serializeTo(ByteBuffer buffer) {
        int length = 0;
        length += ReadWriteIoUtils.write(MetaMarker.ChunkHeader, buffer);
        length += ReadWriteIoUtils.write(measurementID, buffer);
        length += ReadWriteIoUtils.write(dataSize, buffer);
        length += ReadWriteIoUtils.write(dataType, buffer);
        length += ReadWriteIoUtils.write(numOfPages, buffer);
        length += ReadWriteIoUtils.write(compressionType, buffer);
        length += ReadWriteIoUtils.write(encodingType, buffer);
        length += ReadWriteIoUtils.write(maxTombstoneTime, buffer);
        assert length == getSerializedSize();
        return length;
    }

    /**
     * @param inputStream
     * @param markerRead
     *            Whether the marker of the ChunkHeader has been read
     * @return
     * @throws IOException
     */
    public static ChunkHeader deserializeFrom(InputStream inputStream, boolean markerRead) throws IOException {
        if (!markerRead) {
            byte marker = (byte) inputStream.read();
            if (marker != MARKER)
                MetaMarker.handleUnexpectedMarker(marker);
        }

        String measurementID = ReadWriteIoUtils.readString(inputStream);
        int dataSize = ReadWriteIoUtils.readInt(inputStream);
        TSDataType dataType = TSDataType.deserialize(ReadWriteIoUtils.readShort(inputStream));
        int numOfPages = ReadWriteIoUtils.readInt(inputStream);
        CompressionType type = ReadWriteIoUtils.readCompressionType(inputStream);
        TSEncoding encoding = ReadWriteIoUtils.readEncoding(inputStream);
        long maxTombstoneTime = ReadWriteIoUtils.readLong(inputStream);
        return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages, maxTombstoneTime);
    }

    public static ChunkHeader deserializeFrom(ByteBuffer byteBuffer, boolean markerRead) throws IOException {
        if (!markerRead) {
            byte marker = byteBuffer.get();
            if (marker != MARKER)
                MetaMarker.handleUnexpectedMarker(marker);
        }

        String measurementID = ReadWriteIoUtils.readString(byteBuffer);
        return deserializePartFrom(measurementID, byteBuffer);
    }

    public static ChunkHeader deserializeFrom(FileChannel channel, long offset, boolean markerRead) throws IOException {
        if (!markerRead) {
            offset++;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, offset);
        buffer.flip();
        int size = buffer.getInt();
        offset += Integer.BYTES;
        buffer = ByteBuffer.allocate(getSerializedSize(size));
        ReadWriteIoUtils.readAsPossible(channel, offset, buffer);
        buffer.flip();
        String measurementID = ReadWriteIoUtils.readStringWithoutLength(buffer, size);
        return deserializePartFrom(measurementID, buffer);
    }

    private static ChunkHeader deserializePartFrom(String measurementID, ByteBuffer buffer) {
        int dataSize = ReadWriteIoUtils.readInt(buffer);
        TSDataType dataType = TSDataType.deserialize(ReadWriteIoUtils.readShort(buffer));
        int numOfPages = ReadWriteIoUtils.readInt(buffer);
        CompressionType type = ReadWriteIoUtils.readCompressionType(buffer);
        TSEncoding encoding = ReadWriteIoUtils.readEncoding(buffer);
        long maxTombstoneTime = ReadWriteIoUtils.readLong(buffer);
        return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages, maxTombstoneTime);
    }

    public int getNumOfPages() {
        return numOfPages;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public TSEncoding getEncodingType() {
        return encodingType;
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    public void setMaxTombstoneTime(long maxTombstoneTime) {
        this.maxTombstoneTime = maxTombstoneTime;
    }

    public static int getSerializedSize(String measurementID) {
        return Byte.BYTES + Integer.BYTES + getSerializedSize(measurementID.length());
    }

    private static int getSerializedSize(int measurementIDLength) {
        return measurementIDLength + Integer.BYTES + TSDataType.getSerializedSize() + Integer.BYTES
                + CompressionType.getSerializedSize() + TSEncoding.getSerializedSize() + Long.BYTES;
    }

    @Override
    public String toString() {
        return "ChunkHeader{" + "measurementID='" + measurementID + '\'' + ", dataSize=" + dataSize + ", dataType="
                + dataType + ", compressionType=" + compressionType + ", encodingType=" + encodingType + ", numOfPages="
                + numOfPages + ", serializedSize=" + serializedSize + '}';
    }
}
