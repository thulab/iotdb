/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TsDeviceMetadataIndex {

  /**
   * The offset of the TsDeviceMetadata.
   */
  private long offset;
  /**
   * The size of the TsDeviceMetadata in the disk.
   */
  private int len;
  /**
   * The start time of the device.
   */
  private long startTime;
  /**
   * The end time of the device.
   */
  private long endTime;

  public TsDeviceMetadataIndex() {

  }

  /**
   * construct function for TsDeviceMetadataIndex.
   *
   * @param offset -use to initial offset
   * @param len -use to initial len
   * @param deviceMetadata -use to initial startTime and endTime
   */
  public TsDeviceMetadataIndex(long offset, int len, TsDeviceMetadata deviceMetadata) {
    this.offset = offset;
    this.len = len;
    this.startTime = deviceMetadata.getStartTime();
    this.endTime = deviceMetadata.getEndTime();
  }

  /**
   * use inputStream to get a TsDeviceMetadataIndex.
   *
   * @param inputStream -determine the index's source
   * @return -a TsDeviceMetadataIndex
   */
  public static TsDeviceMetadataIndex deserializeFrom(InputStream inputStream) throws IOException {
    TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
    index.offset = ReadWriteIOUtils.readLong(inputStream);
    index.len = ReadWriteIOUtils.readInt(inputStream);
    index.startTime = ReadWriteIOUtils.readLong(inputStream);
    index.endTime = ReadWriteIOUtils.readLong(inputStream);
    return index;
  }

  /**
   * use buffer to get a TsDeviceMetadataIndex.
   *
   * @param buffer -determine the index's source
   * @return -a TsDeviceMetadataIndex
   */
  public static TsDeviceMetadataIndex deserializeFrom(ByteBuffer buffer) {
    TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
    index.offset = ReadWriteIOUtils.readLong(buffer);
    index.len = ReadWriteIOUtils.readInt(buffer);
    index.startTime = ReadWriteIOUtils.readLong(buffer);
    index.endTime = ReadWriteIOUtils.readLong(buffer);
    return index;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLen() {
    return len;
  }

  public void setLen(int len) {
    this.len = len;
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

  /**
   * get the byte length of the given outputStream.
   *
   * @param outputStream -param to determine the byte length
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(offset, outputStream);
    byteLen += ReadWriteIOUtils.write(len, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    return byteLen;
  }

  /**
   * get the byte length of the given buffer.
   *
   * @param buffer -param to determine the byte length
   * @return -byte length
   */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(offset, buffer);
    byteLen += ReadWriteIOUtils.write(len, buffer);
    byteLen += ReadWriteIOUtils.write(startTime, buffer);
    byteLen += ReadWriteIOUtils.write(endTime, buffer);
    return byteLen;
  }

  @Override
  public String toString() {
    return "TsDeviceMetadataIndex{" + "offset=" + offset + ", len=" + len + ", startTime="
        + startTime
        + ", endTime=" + endTime + '}';
  }
}
