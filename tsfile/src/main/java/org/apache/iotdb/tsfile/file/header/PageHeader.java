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

import org.apache.iotdb.tsfile.file.metadata.statistics.NoStatistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIoUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class PageHeader {

    private int uncompressedSize;
    private int compressedSize;
    private int numOfValues;
    private Statistics<?> statistics;
    private long max_timestamp;
    private long min_timestamp;

    // this field does not need to be serialized.
    private int serializedSize;

    public static int calculatePageHeaderSize(TSDataType type) {
        return 3 * Integer.BYTES + 2 * Long.BYTES + Statistics.getStatsByType(type).getSerializedSize();
    }

    public int calculatePageHeaderSize() {
        return 3 * Integer.BYTES + 2 * Long.BYTES + statistics.getSerializedSize();
    }

    public int getSerializedSize() {
        return serializedSize;
    }

    public PageHeader(int uncompressedSize, int compressedSize, int numOfValues, Statistics<?> statistics,
            long max_timestamp, long min_timestamp) {
        this.uncompressedSize = uncompressedSize;
        this.compressedSize = compressedSize;
        this.numOfValues = numOfValues;
        if (statistics == null)
            statistics = new NoStatistics();
        this.statistics = statistics;
        this.max_timestamp = max_timestamp;
        this.min_timestamp = min_timestamp;
        serializedSize = calculatePageHeaderSize();
    }

    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public void setUncompressedSize(int uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }

    public int getCompressedSize() {
        return compressedSize;
    }

    public void setCompressedSize(int compressedSize) {
        this.compressedSize = compressedSize;
    }

    public int getNumOfValues() {
        return numOfValues;
    }

    public void setNumOfValues(int numOfValues) {
        this.numOfValues = numOfValues;
    }

    public Statistics<?> getStatistics() {
        return statistics;
    }

    /*
     * public void setStatistics(Statistics<?> statistics) { this.statistics = statistics; if(statistics != null)
     * serializedSize = calculatePageHeaderSize(); }
     */

    public long getMax_timestamp() {
        return max_timestamp;
    }

    public void setMax_timestamp(long max_timestamp) {
        this.max_timestamp = max_timestamp;
    }

    public long getMin_timestamp() {
        return min_timestamp;
    }

    public void setMin_timestamp(long min_timestamp) {
        this.min_timestamp = min_timestamp;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int length = 0;
        length += ReadWriteIoUtils.write(uncompressedSize, outputStream);
        length += ReadWriteIoUtils.write(compressedSize, outputStream);
        length += ReadWriteIoUtils.write(numOfValues, outputStream);
        length += ReadWriteIoUtils.write(max_timestamp, outputStream);
        length += ReadWriteIoUtils.write(min_timestamp, outputStream);
        length += statistics.serialize(outputStream);
        assert length == getSerializedSize();
        return length;
    }

    public static PageHeader deserializeFrom(InputStream inputStream, TSDataType dataType) throws IOException {
        int uncompressedSize = ReadWriteIoUtils.readInt(inputStream);
        int compressedSize = ReadWriteIoUtils.readInt(inputStream);
        int numOfValues = ReadWriteIoUtils.readInt(inputStream);
        long max_timestamp = ReadWriteIoUtils.readLong(inputStream);
        long min_timestamp = ReadWriteIoUtils.readLong(inputStream);
        Statistics statistics = Statistics.deserialize(inputStream, dataType);
        return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, max_timestamp, min_timestamp);
    }

    public static PageHeader deserializeFrom(ByteBuffer buffer, TSDataType dataType) throws IOException {
        int uncompressedSize = ReadWriteIoUtils.readInt(buffer);
        int compressedSize = ReadWriteIoUtils.readInt(buffer);
        int numOfValues = ReadWriteIoUtils.readInt(buffer);
        long max_timestamp = ReadWriteIoUtils.readLong(buffer);
        long min_timestamp = ReadWriteIoUtils.readLong(buffer);
        Statistics statistics = Statistics.deserialize(buffer, dataType);
        return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, max_timestamp, min_timestamp);
    }

    @Override
    public String toString() {
        return "PageHeader{" + "uncompressedSize=" + uncompressedSize + ", compressedSize=" + compressedSize
                + ", numOfValues=" + numOfValues + ", statistics=" + statistics + ", max_timestamp=" + max_timestamp
                + ", min_timestamp=" + min_timestamp + ", serializedSize=" + serializedSize + '}';
    }
}
