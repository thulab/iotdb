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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Statistics for double type
 *
 * @author kangrong
 */
public class DoubleStatistics extends Statistics<Double> {
    private double max;
    private double min;
    private double first;
    private double sum;
    private double last;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToDouble(maxBytes);
        min = BytesUtils.bytesToDouble(minBytes);
    }

    @Override
    public void updateStats(double value) {
        if (this.isEmpty) {
            initializeStats(value, value, value, value, value);
            isEmpty = false;
        } else {
            updateStats(value, value, value, value, value);
        }
    }

    private void updateStats(double minValue, double maxValue, double firstValue, double sumValue, double lastValue) {
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
    public Double getMax() {
        return max;
    }

    @Override
    public Double getMin() {
        return min;
    }

    @Override
    public Double getFirst() {
        return first;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public Double getLast() {
        return last;
    }

    @Override
    protected void mergeStatisticsValue(Statistics<?> stats) {
        DoubleStatistics doubleStats = (DoubleStatistics) stats;
        if (this.isEmpty) {
            initializeStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(), doubleStats.getSum(),
                    doubleStats.getLast());
            isEmpty = false;
        } else {
            updateStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(), doubleStats.getSum(),
                    doubleStats.getLast());
        }

    }

    public void initializeStats(double min, double max, double first, double sum, double last) {
        this.min = min;
        this.max = max;
        this.first = first;
        this.sum = sum;
        this.last = last;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.doubleToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.doubleToBytes(min);
    }

    @Override
    public byte[] getFirstBytes() {
        return BytesUtils.doubleToBytes(first);
    }

    @Override
    public byte[] getSumBytes() {
        return BytesUtils.doubleToBytes(sum);
    }

    @Override
    public byte[] getLastBytes() {
        return BytesUtils.doubleToBytes(last);
    }

    @Override
    public ByteBuffer getMaxBytebuffer() {
        return ReadWriteIoUtils.getByteBuffer(max);
    }

    @Override
    public ByteBuffer getMinBytebuffer() {
        return ReadWriteIoUtils.getByteBuffer(min);
    }

    @Override
    public ByteBuffer getFirstBytebuffer() {
        return ReadWriteIoUtils.getByteBuffer(first);
    }

    @Override
    public ByteBuffer getSumBytebuffer() {
        return ReadWriteIoUtils.getByteBuffer(sum);
    }

    @Override
    public ByteBuffer getLastBytebuffer() {
        return ReadWriteIoUtils.getByteBuffer(last);
    }

    @Override
    public int sizeOfDatum() {
        return 8;
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last + "]";
    }

    @Override
    void fill(InputStream inputStream) throws IOException {
        this.min = ReadWriteIoUtils.readDouble(inputStream);
        this.max = ReadWriteIoUtils.readDouble(inputStream);
        this.first = ReadWriteIoUtils.readDouble(inputStream);
        this.last = ReadWriteIoUtils.readDouble(inputStream);
        this.sum = ReadWriteIoUtils.readDouble(inputStream);
    }

    @Override
    void fill(ByteBuffer byteBuffer) throws IOException {
        this.min = ReadWriteIoUtils.readDouble(byteBuffer);
        this.max = ReadWriteIoUtils.readDouble(byteBuffer);
        this.first = ReadWriteIoUtils.readDouble(byteBuffer);
        this.last = ReadWriteIoUtils.readDouble(byteBuffer);
        this.sum = ReadWriteIoUtils.readDouble(byteBuffer);
    }
}
