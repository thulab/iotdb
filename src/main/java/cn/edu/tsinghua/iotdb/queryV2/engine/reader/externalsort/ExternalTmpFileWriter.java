package cn.edu.tsinghua.iotdb.queryV2.engine.reader.externalsort;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.IOException;

/**
 * Write TimeValuePairs from the result of merge sort of some SeriesChunks to external temporal file.
 * Created by zhangjinrui on 2018/1/15.
 */
public interface ExternalTmpFileWriter {

    void write(TimeValuePair timeValuePair) throws IOException;
}
