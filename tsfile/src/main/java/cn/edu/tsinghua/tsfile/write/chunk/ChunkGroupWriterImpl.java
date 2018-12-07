package cn.edu.tsinghua.tsfile.write.chunk;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import cn.edu.tsinghua.tsfile.exception.write.NoMeasurementException;
import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.write.record.datapoint.DataPoint;

/**
 * a implementation of IChunkGroupWriter
 */
public class ChunkGroupWriterImpl implements IChunkGroupWriter {

    private static Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);

    private final String deviceId;

    /**
     * <measurementID, ChunkWriterImpl>
     */
    private Map<String, IChunkWriter> chunkWriters = new HashMap<>();

    public ChunkGroupWriterImpl(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public void addSeriesWriter(MeasurementSchema schema, int pageSizeThreshold) {
        if (!chunkWriters.containsKey(schema.getMeasurementId())) {
            ChunkBuffer chunkBuffer = new ChunkBuffer(schema);
            IChunkWriter seriesWriter = new ChunkWriterImpl(schema, chunkBuffer, pageSizeThreshold);
            this.chunkWriters.put(schema.getMeasurementId(), seriesWriter);
        }
    }

    @Override
    public void write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
        for (DataPoint point : data) {
            String measurementId = point.getMeasurementId();
            if (!chunkWriters.containsKey(measurementId))
                throw new NoMeasurementException("time " + time + ", measurement id " + measurementId + " not found!");
            point.writeTo(time, chunkWriters.get(measurementId));

        }
    }

    @Override
    public void flushToFileWriter(TsFileIOWriter fileWriter) throws IOException {
        LOG.debug("start flush device id:{}", deviceId);
        for (IChunkWriter seriesWriter : chunkWriters.values()) {
            seriesWriter.writeToFileWriter(fileWriter);
        }
    }

    @Override
    public long updateMaxGroupMemSize() {
        long bufferSize = 0;
        for (IChunkWriter seriesWriter : chunkWriters.values())
            bufferSize += seriesWriter.estimateMaxSeriesMemSize();
        return bufferSize;
    }


    @Override
    public long getCurrentChunkGroupSize() {
        long size = 0;
        for (IChunkWriter writer : chunkWriters.values()) {
            size += writer.getCurrentChunkSize();
        }
        return size;
    }

    @Override
    public void preFlush() {
        for (IChunkWriter writer : chunkWriters.values()) {
            writer.preFlush();
        }
    }

    @Override
    public int getSeriesNumber() {
        return chunkWriters.size();
    }
}
