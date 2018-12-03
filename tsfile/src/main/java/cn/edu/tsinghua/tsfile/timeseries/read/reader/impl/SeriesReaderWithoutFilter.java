package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;

import java.io.IOException;
import java.util.List;


public class SeriesReaderWithoutFilter extends SeriesReader {

    public SeriesReaderWithoutFilter(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(chunkLoader, chunkMetaDataList);
    }

    public SeriesReaderWithoutFilter(TsFileSequenceReader tsFileReader, Path path) throws IOException {
        super(tsFileReader, path);
    }

    public SeriesReaderWithoutFilter(TsFileSequenceReader tsFileReader,
                                     ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(tsFileReader, chunkLoader, chunkMetaDataList);
    }

    protected void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderWithoutFilter(chunk);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        return true;
    }

}
