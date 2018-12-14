package cn.edu.tsinghua.iotdb.queryV2.reader.sequence;

import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePairUtils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.UnClosedTsFileReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;

public class UnSealedTsFileReader implements IReader {

    protected Path seriesPath;
    private FileSeriesReader tsFileReader;
    private BatchData data;

    public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile, Filter filter) throws IOException {

        ChunkLoader chunkLoader = new ChunkLoaderImpl(new UnClosedTsFileReader(unsealedTsFile.getFilePath()));

        if (filter == null) {
            tsFileReader = new FileSeriesReaderWithoutFilter(chunkLoader, unsealedTsFile.getChunkMetaDataList());
        } else {
            tsFileReader = new FileSeriesReaderWithFilter(chunkLoader, unsealedTsFile.getChunkMetaDataList(), filter);
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        if (data == null)
            data = tsFileReader.nextBatch();

        if (!data.hasNext() && !tsFileReader.hasNextBatch())
            return false;

        while (!data.hasNext()) {
            data = tsFileReader.nextBatch();
            if (data.hasNext())
                return true;
        }

        return false;
    }

    @Override
    public TimeValuePair next() {
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
        data.next();
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() {
        data.next();
    }

    @Override
    public void close() throws IOException {
        if (tsFileReader != null) {
            tsFileReader.close();
        }
    }

    @Override
    public boolean hasNextBatch() {
        return false;
    }

    @Override
    public BatchData nextBatch() {
        return null;
    }

    @Override
    public BatchData currentBatch() {
        return null;
    }
}
