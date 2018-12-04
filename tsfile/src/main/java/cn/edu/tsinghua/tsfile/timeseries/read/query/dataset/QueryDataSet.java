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

    /**
     * This method is used for batch query.
     */
    boolean hasNextV2() throws IOException;

    /**
     * This method is used for batch query, return RowRecordV2.
     */
    RowRecordV2 nextV2() throws IOException;

}
