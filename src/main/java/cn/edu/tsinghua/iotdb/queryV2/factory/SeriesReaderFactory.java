package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReader;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class SeriesReaderFactory {

    private long jodId = 0;
    private OverflowSeriesChunkLoader overflowSeriesChunkLoader;

    public SeriesReaderFactory() {
        overflowSeriesChunkLoader = new OverflowSeriesChunkLoader();
    }

    public OverflowInsertDataReader createSeriesReaderForOverflowInsert(OverflowSeriesDataSource overflowSeriesDataSource, Filter<?> filter) {
        long jobId = getNextJobId();
        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptorList = SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowSeriesDataSource.getOverflowInsertFileList());

        return null;
    }

    private SeriesChunkReader createSeriesChunkReader(SeriesChunkDescriptor seriesChunkDescriptor) {
        long jobId = getNextJobId();
        return null;
    }

    private synchronized long getNextJobId() {
        jodId++;
        return jodId;
    }
}
