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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TsDeviceMetadataIndex {
    /**
     * The offset of the TsDeviceMetadata
     */
    private long offset;
    /**
     * The size of the TsDeviceMetadata in the disk
     */
    private int len;
    /**
     * The start time of the device
     */
    private long startTime;
    /**
     * The end time of the device
     */
    private long endTime;

    public TsDeviceMetadataIndex() {

    }

    public TsDeviceMetadataIndex(long offset, int len, TsDeviceMetadata deviceMetadata) {
        this.offset = offset;
        this.len = len;
        this.startTime = deviceMetadata.getStartTime();
        this.endTime = deviceMetadata.getEndTime();
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getOffset() {
        return offset;
    }

    public int getLen() {
        return len;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIoUtils.write(offset, outputStream);
        byteLen += ReadWriteIoUtils.write(len, outputStream);
        byteLen += ReadWriteIoUtils.write(startTime, outputStream);
        byteLen += ReadWriteIoUtils.write(endTime, outputStream);
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIoUtils.write(offset, buffer);
        byteLen += ReadWriteIoUtils.write(len, buffer);
        byteLen += ReadWriteIoUtils.write(startTime, buffer);
        byteLen += ReadWriteIoUtils.write(endTime, buffer);
        return byteLen;
    }

    public static TsDeviceMetadataIndex deserializeFrom(InputStream inputStream) throws IOException {
        TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
        index.offset = ReadWriteIoUtils.readLong(inputStream);
        index.len = ReadWriteIoUtils.readInt(inputStream);
        index.startTime = ReadWriteIoUtils.readLong(inputStream);
        index.endTime = ReadWriteIoUtils.readLong(inputStream);
        return index;
    }

    public static TsDeviceMetadataIndex deserializeFrom(ByteBuffer buffer) throws IOException {
        TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
        index.offset = ReadWriteIoUtils.readLong(buffer);
        index.len = ReadWriteIoUtils.readInt(buffer);
        index.startTime = ReadWriteIoUtils.readLong(buffer);
        index.endTime = ReadWriteIoUtils.readLong(buffer);
        return index;
    }

    @Override
    public String toString() {
        return "TsDeviceMetadataIndex{" + "offset=" + offset + ", len=" + len + ", startTime=" + startTime
                + ", endTime=" + endTime + '}';
    }
}
