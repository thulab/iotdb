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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for long type.
 *
 * @author kangrong
 */
public class LongStatistics extends Statistics<Long> {

  private long max;
  private long min;
  private long first;
  private double sum;
  private long last;

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToLong(maxBytes);
    min = BytesUtils.bytesToLong(minBytes);
  }

  @Override
  public Long getMin() {
    return min;
  }

  @Override
  public Long getMax() {
    return max;
  }

  @Override
  public Long getFirst() {
    return first;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  public Long getLast() {
    return last;
  }

  @Override
  public void updateStats(long value) {
    if (isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value, value);
    }
  }

  private void updateStats(long minValue, long maxValue, long firstValue, double sumValue,
      long lastValue) {
    if (minValue < min) {
      min = minValue;
    }
    if (maxValue > max) {
      max = maxValue;
    }
    sum += sumValue;
    this.last = lastValue;
  }

  @Override
  public void updateStats(long minValue, long maxValue) {
    if (minValue < min) {
      min = minValue;
    }
    if (maxValue > max) {
      max = maxValue;
    }
  }

  @Override
  protected void mergeStatisticsValue(Statistics<?> stats) {
    LongStatistics longStats = (LongStatistics) stats;
    if (isEmpty) {
      initializeStats(longStats.getMin(), longStats.getMax(), longStats.getFirst(),
          longStats.getSum(),
          longStats.getLast());
      isEmpty = false;
    } else {
      updateStats(longStats.getMin(), longStats.getMax(), longStats.getFirst(), longStats.getSum(),
          longStats.getLast());
    }

  }

  void initializeStats(long min, long max, long firstValue, double sum, long last) {
    this.min = min;
    this.max = max;
    this.first = firstValue;
    this.sum += sum;
    this.last = last;
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(min);
  }

  @Override
  public byte[] getFirstBytes() {
    return BytesUtils.longToBytes(first);
  }

  @Override
  public byte[] getSumBytes() {
    return BytesUtils.doubleToBytes(sum);
  }

  @Override
  public byte[] getLastBytes() {
    return BytesUtils.longToBytes(last);
  }

  @Override
  public ByteBuffer getMaxBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(max);
  }

  @Override
  public ByteBuffer getMinBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(min);
  }

  @Override
  public ByteBuffer getFirstBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(first);
  }

  @Override
  public ByteBuffer getSumBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(sum);
  }

  @Override
  public ByteBuffer getLastBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(last);
  }

  @Override
  public String toString() {
    return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last
        + "]";
  }

  @Override
  public int sizeOfDatum() {
    return 8;
  }

  @Override
  void fill(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readLong(inputStream);
    this.max = ReadWriteIOUtils.readLong(inputStream);
    this.first = ReadWriteIOUtils.readLong(inputStream);
    this.last = ReadWriteIOUtils.readLong(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void fill(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readLong(byteBuffer);
    this.max = ReadWriteIOUtils.readLong(byteBuffer);
    this.first = ReadWriteIOUtils.readLong(byteBuffer);
    this.last = ReadWriteIOUtils.readLong(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }

}
