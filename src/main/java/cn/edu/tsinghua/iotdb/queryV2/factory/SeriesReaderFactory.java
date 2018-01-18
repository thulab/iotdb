package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReader;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class SeriesReaderFactory {
    public SeriesReader createSeriesReaderForOverflowInsert(OverflowSeriesDataSource overflowSeriesDataSource, Filter<?> filter) {
        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptorList = SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowSeriesDataSource.getOverflowInsertFileList());

        return null;
    }

    private SeriesChunkReader createSeriesChunkReader(SeriesChunkDescriptor seriesChunkDescriptor) {
        return null;
    }
}
