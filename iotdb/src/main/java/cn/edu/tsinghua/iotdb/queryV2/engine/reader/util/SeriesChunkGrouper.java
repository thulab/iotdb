package cn.edu.tsinghua.iotdb.queryV2.engine.reader.util;

import cn.edu.tsinghua.tsfile.read.common.SeriesChunkDescriptor;

import java.util.List;

/**
 * Group all SeriesDescriptor by respective timestamp interval. The SeriesDescriptors whose timestamp intervals are overlapped will
 * be divided into one group.
 */
public interface SeriesChunkGrouper {
    List<List<SeriesChunkDescriptor>> group(List<SeriesChunkDescriptor> seriesChunkDescriptorList);
}
