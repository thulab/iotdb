package cn.edu.tsinghua.iotdb.queryV2.engine.reader.sequence;

import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.iotdb.read.Utils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.List;

public class UnSealedTsFileReader implements IReader {

    protected Path seriesPath;
    private FileSeriesReader tsFileReader;
    private BatchData data;
    private SingleSeriesExpression singleSeriesExpression;

    public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile) throws IOException {

        ChunkLoader chunkLoader = new ChunkLoaderImpl(new TsFileSequenceReader(unsealedTsFile.getFilePath(), false));

        initSingleTsFileReader(chunkLoader, unsealedTsFile.getTimeSeriesChunkMetaDatas());
    }

    public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile, SingleSeriesExpression singleSeriesExpression) throws IOException {
        this(unsealedTsFile);

        this.singleSeriesExpression = singleSeriesExpression;
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
        TimeValuePair timeValuePair = Utils.getCurrenTimeValuePair(data);
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

    protected void initSingleTsFileReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaData) {
        if (singleSeriesExpression == null) {
            tsFileReader = new FileSeriesReaderWithoutFilter(chunkLoader, chunkMetaData);
        } else {
            tsFileReader = new FileSeriesReaderWithFilter(chunkLoader, chunkMetaData, singleSeriesExpression.getFilter());
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
