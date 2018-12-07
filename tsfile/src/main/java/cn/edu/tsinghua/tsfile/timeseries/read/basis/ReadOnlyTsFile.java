package cn.edu.tsinghua.tsfile.timeseries.read.basis;

import cn.edu.tsinghua.tsfile.timeseries.read.reader.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.executor.TsFileExecutor;

import java.io.IOException;


public class ReadOnlyTsFile {

    private TsFileSequenceReader fileReader;
    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;
    private TsFileExecutor tsFileExecutor;

    public ReadOnlyTsFile(TsFileSequenceReader fileReader) throws IOException {
        this.fileReader = fileReader;
        this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
        this.chunkLoader = new ChunkLoaderImpl(fileReader);
        tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
    }

    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        return tsFileExecutor.execute(queryExpression);
    }

    public void close() throws IOException {
        fileReader.close();
    }
}
