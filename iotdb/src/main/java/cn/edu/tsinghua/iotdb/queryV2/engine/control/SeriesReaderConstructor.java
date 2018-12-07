package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;


public interface SeriesReaderConstructor {
    SeriesReader create(QueryDataSource queryDataSource);

    SeriesReader create(QueryDataSource queryDataSource, Filter<?> filter);

    SeriesReader createSeriesReaderByTimestamp(QueryDataSource queryDataSource);
}
