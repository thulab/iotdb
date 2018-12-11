package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PrioritySeriesReader;

import java.util.List;

/**
 * Merge all SeriesChunks to one SeriesChunk using external sort and return corresponding SeriesChunkReader
 */
public class ExternalSortSeriesChunkReaderGenerator {

    public ExternalSortSeriesChunkReaderGenerator(List<PrioritySeriesReader> seriesChunkReaderList) {

    }

    public PrioritySeriesReader generate() {
        return null;
    }
}
