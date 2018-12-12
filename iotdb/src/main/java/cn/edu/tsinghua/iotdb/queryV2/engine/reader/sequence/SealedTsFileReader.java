package cn.edu.tsinghua.iotdb.queryV2.engine.reader.sequence;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.iotdb.read.Utils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SealedTsFileReader implements IReader {

    private Path seriesPath;
    private List<IntervalFileNode> sealedTsFiles;
    private int usedIntervalFileIndex;
    private FileSeriesReader seriesReader;
    private SingleSeriesExpression singleSeriesExpression;
    private BatchData data;
    private boolean hasCachedData;


    public SealedTsFileReader(GlobalSortedSeriesDataSource sortedSeriesDataSource, SingleSeriesExpression singleSeriesExpression) {
        this(sortedSeriesDataSource);
        this.singleSeriesExpression = singleSeriesExpression;
    }


    public SealedTsFileReader(GlobalSortedSeriesDataSource sortedSeriesDataSource) {
        this.seriesPath = sortedSeriesDataSource.getSeriesPath();
        this.sealedTsFiles = sortedSeriesDataSource.getSealedTsFiles();
        this.usedIntervalFileIndex = -1;
        this.seriesReader = null;
        this.hasCachedData = false;
    }

    public SealedTsFileReader(FileSeriesReader seriesReader) {
        this.seriesReader = seriesReader;
        sealedTsFiles = new ArrayList<>();
    }


    @Override
    public boolean hasNext() throws IOException {

        while (!hasCachedData) {

            // try to get next time value pair from current data
            if (data != null && data.hasNext()) {
                hasCachedData = true;
                return true;
            }

            // try to get next batch data from current reader
            if (seriesReader != null && seriesReader.hasNextBatch()) {
                data = seriesReader.nextBatch();
                if (data.hasNext()) {
                    hasCachedData = true;
                    return true;
                } else {
                    continue;
                }
            }

            // try to get next batch data from next reader
            while ((usedIntervalFileIndex + 1) < sealedTsFiles.size()) {
                // init until reach a satisfied reader
                if (seriesReader == null || !seriesReader.hasNextBatch()) {
                    IntervalFileNode fileNode = sealedTsFiles.get(++usedIntervalFileIndex);
                    if (singleTsFileSatisfied(fileNode)) {
                        initSingleTsFileReader(fileNode);
                    } else {
                        continue;
                    }
                }
                if (seriesReader.hasNextBatch()) {
                    data = seriesReader.nextBatch();
                }
            }
        }

        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        TimeValuePair timeValuePair = Utils.getCurrenTimeValuePair(data);
        data.next();
        hasCachedData = false;
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        if (seriesReader != null) {
            seriesReader.close();
        }
    }

    private boolean singleTsFileSatisfied(IntervalFileNode fileNode) {

        if (singleSeriesExpression == null)
            return true;

        // TODO

        return true;
    }

    private void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {
        TsFileSequenceReader tsFileReader = new TsFileSequenceReader(fileNode.getFilePath(), true);
        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

        if (singleSeriesExpression == null) {
            seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
        } else {
            seriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, singleSeriesExpression.getFilter());
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
