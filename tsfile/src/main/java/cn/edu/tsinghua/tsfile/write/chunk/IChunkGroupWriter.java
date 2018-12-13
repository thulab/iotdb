package cn.edu.tsinghua.tsfile.write.chunk;

import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.write.record.datapoint.DataPoint;

import java.io.IOException;
import java.util.List;

/**
 * a chunk group in TSFile contains a list of value series. TimeSeriesGroupWriter
 * should implement write method which inputs a time stamp(in TimeValue class)
 * and a list of data points. It also should provide flushing method for
 * outputting to OS file system or HDFS.
 *
 * @author kangrong
 */
public interface IChunkGroupWriter {
    /**
     * receive a timestamp and a list of data points, write them to their
     * series writers.
     *
     * @param time - all data points have unify time stamp.
     * @param data - data point list to input
     * @throws WriteProcessException exception in write process
     * @throws IOException exception in IO
     */
    void write(long time, List<DataPoint> data) throws WriteProcessException, IOException;

    /**
     * flushing method for outputting to OS file system or HDFS.
     * Implemented by ChunkWriterImpl.writeToFileWriter()
     *
     * @param tsfileWriter - TSFileIOWriter
     * @throws IOException exception in IO
     */
    void flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

    /**
     * get the max memory occupied at this time.
     *
     * Note that, this method should be called after running
     * {@code long calcAllocatedSize()}
     *
     * @return - allocated memory size.
     */
    long updateMaxGroupMemSize();

    /**
     * given a measurement descriptor, create a corresponding writer and put into this ChunkGroupWriter
     *
     * @param measurementSchema a measurement descriptor containing the message of the series
     * @param pageSize the specified page size
     */
    void addSeriesWriter(MeasurementSchema measurementSchema, int pageSize);

    /**
     * @return get the serialized size of current chunkGroup header + all chunks
     */
    long getCurrentChunkGroupSize();

    /**
     * call all the series to prepare to flush data.
     */
    void preFlush();

    int getSeriesNumber();
}