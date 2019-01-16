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
package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.ReadWriteIoUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * MetaData of one chunk
 */
public class ChunkMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkMetaData.class);

    private String measurementUID;

    /**
     * Byte offset of the corresponding data in the file Notice: include the chunk header and marker
     */
    private long offsetOfChunkHeader;

    private long numOfPoints;

    private long startTime;

    private long endTime;

    private TSDataType tsDataType;

    /**
     * The maximum time of the tombstones that take effect on this chunk. Only data with larger timestamps than this
     * should be exposed to user.
     */
    private long maxTombstoneTime;

    private TsDigest valuesStatistics;

    public int getSerializedSize() {
        return (Integer.BYTES + measurementUID.length()) + // measurementUID
                4 * Long.BYTES + // 4 long: offsetOfChunkHeader, numOfPoints, startTime, endTime
                TSDataType.getSerializedSize() + // TSDataType
                (valuesStatistics == null ? TsDigest.getNullDigestSize() : valuesStatistics.getSerializedSize());

    }

    private ChunkMetaData() {
    }

    public ChunkMetaData(String measurementUID, TSDataType tsDataType, long fileOffset, long startTime, long endTime) {
        this.measurementUID = measurementUID;
        this.tsDataType = tsDataType;
        this.offsetOfChunkHeader = fileOffset;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return String.format("numPoints %d", numOfPoints);
    }

    public long getNumOfPoints() {
        return numOfPoints;
    }

    public void setNumOfPoints(long numRows) {
        this.numOfPoints = numRows;
    }

    /**
     * @return Byte offset of header of this chunk (includes the marker)
     */
    public long getOffsetOfChunkHeader() {
        return offsetOfChunkHeader;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public TsDigest getDigest() {
        return valuesStatistics;
    }

    public void setDigest(TsDigest digest) {
        this.valuesStatistics = digest;

    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public TSDataType getTsDataType() {
        return tsDataType;
    }

    public void setTsDataType(TSDataType tsDataType) {
        this.tsDataType = tsDataType;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIoUtils.write(measurementUID, outputStream);
        byteLen += ReadWriteIoUtils.write(offsetOfChunkHeader, outputStream);
        byteLen += ReadWriteIoUtils.write(numOfPoints, outputStream);
        byteLen += ReadWriteIoUtils.write(startTime, outputStream);
        byteLen += ReadWriteIoUtils.write(endTime, outputStream);
        byteLen += ReadWriteIoUtils.write(tsDataType, outputStream);

        if (valuesStatistics == null)
            byteLen += TsDigest.serializeNullTo(outputStream);
        else
            byteLen += valuesStatistics.serializeTo(outputStream);

        assert byteLen == getSerializedSize();
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) {
        int byteLen = 0;

        byteLen += ReadWriteIoUtils.write(measurementUID, buffer);
        byteLen += ReadWriteIoUtils.write(offsetOfChunkHeader, buffer);
        byteLen += ReadWriteIoUtils.write(numOfPoints, buffer);
        byteLen += ReadWriteIoUtils.write(startTime, buffer);
        byteLen += ReadWriteIoUtils.write(endTime, buffer);
        byteLen += ReadWriteIoUtils.write(tsDataType, buffer);

        if (valuesStatistics == null)
            byteLen += TsDigest.serializeNullTo(buffer);
        else
            byteLen += valuesStatistics.serializeTo(buffer);

        assert byteLen == getSerializedSize();
        return byteLen;
    }

    public static ChunkMetaData deserializeFrom(InputStream inputStream) throws IOException {
        ChunkMetaData chunkMetaData = new ChunkMetaData();

        chunkMetaData.measurementUID = ReadWriteIoUtils.readString(inputStream);

        chunkMetaData.offsetOfChunkHeader = ReadWriteIoUtils.readLong(inputStream);

        chunkMetaData.numOfPoints = ReadWriteIoUtils.readLong(inputStream);
        chunkMetaData.startTime = ReadWriteIoUtils.readLong(inputStream);
        chunkMetaData.endTime = ReadWriteIoUtils.readLong(inputStream);

        chunkMetaData.tsDataType = ReadWriteIoUtils.readDataType(inputStream);

        chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(inputStream);

        return chunkMetaData;
    }

    public static ChunkMetaData deserializeFrom(ByteBuffer buffer) {
        ChunkMetaData chunkMetaData = new ChunkMetaData();

        chunkMetaData.measurementUID = ReadWriteIoUtils.readString(buffer);
        chunkMetaData.offsetOfChunkHeader = ReadWriteIoUtils.readLong(buffer);
        chunkMetaData.numOfPoints = ReadWriteIoUtils.readLong(buffer);
        chunkMetaData.startTime = ReadWriteIoUtils.readLong(buffer);
        chunkMetaData.endTime = ReadWriteIoUtils.readLong(buffer);
        chunkMetaData.tsDataType = ReadWriteIoUtils.readDataType(buffer);

        chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(buffer);

        return chunkMetaData;
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    public void setMaxTombstoneTime(long maxTombstoneTime) {
        this.maxTombstoneTime = maxTombstoneTime;
    }

}
