package cn.edu.tsinghua.iotdb.queryV2.reader.sequence;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.queryV2.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePairUtils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SealedTsFilesReader implements IReader {

    private Path seriesPath;
    private List<IntervalFileNode> sealedTsFiles;
    private int usedIntervalFileIndex;
    private FileSeriesReader seriesReader;
    private Filter filter;
    private BatchData data;
    private boolean hasCachedData;


    public SealedTsFilesReader(Path path, List<IntervalFileNode> sealedTsFiles, Filter filter) {
        this(path, sealedTsFiles);
        this.filter = filter;
    }


    public SealedTsFilesReader(Path path, List<IntervalFileNode> sealedTsFiles) {
        this.seriesPath = path;
        this.sealedTsFiles = sealedTsFiles;
        this.usedIntervalFileIndex = -1;
        this.seriesReader = null;
        this.hasCachedData = false;
    }

    public SealedTsFilesReader(FileSeriesReader seriesReader) {
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
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
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

        if (filter == null)
            return true;

        // TODO

        return true;
    }


    // TODO replace new FileReader by FileReaderManager to avoid file stream limit
    private void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {
        TsFileSequenceReader tsFileReader = new TsFileSequenceReader(fileNode.getFilePath());
        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

        if (filter == null) {
            seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
        } else {
            seriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
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
