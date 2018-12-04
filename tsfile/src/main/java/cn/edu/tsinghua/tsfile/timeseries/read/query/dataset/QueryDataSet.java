package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecordV2;

import java.io.IOException;


public interface QueryDataSet {

    /**
     * check if unread data still exists
     * @return
     * @throws IOException
     */
    boolean hasNext() throws IOException;

    /**
     * get the next unread data
     * another data will be returned when calling this method next time
     * @return
     * @throws IOException
     */
    RowRecord next() throws IOException;

    boolean hasNextV2() throws IOException;

    RowRecordV2 nextV2() throws IOException;

}
