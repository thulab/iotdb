package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.QueryJobManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.mem.MemChunkReaderWithFilter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.mem.MemChunkReaderWithoutFilter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.merge.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.sequence.SealedTsFileReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.unsequence.EngineChunkReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.unsequence.UnSeqSeriesReader;
import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.Chunk;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReader;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SeriesReaderFactory {

    private static final Logger logger = LoggerFactory.getLogger(SeriesReaderFactory.class);
//    private OverflowSeriesChunkLoader overflowSeriesChunkLoader;
    //  private ExternalSortJobEngine externalSortJobEngine;
    private QueryJobManager queryJobManager;

    private SeriesReaderFactory() {
//        overflowSeriesChunkLoader = new OverflowSeriesChunkLoader();
//    externalSortJobEngine = SimpleExternalSortEngine.getInstance();
        queryJobManager = QueryJobManager.getInstance();
    }

    public UnSeqSeriesReader createSeriesReaderForUnSeq(OverflowSeriesDataSource overflowSeriesDataSource, Filter filter)
            throws IOException {
        long jobId = queryJobManager.addJobForOneQuery();
        List<ChunkMetaData> metaDataList = new ArrayList<>();

        PriorityMergeReader priorityMergeReader = new PriorityMergeReader();

        int priorityValue = 1;

        for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource.getOverflowInsertFileList()) {
            TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(overflowInsertFile.getFilePath(), false);
            ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);

            for (ChunkMetaData chunkMetaData : overflowInsertFile.getChunkMetaDataList()) {
                Chunk chunk = chunkLoader.getChunk(chunkMetaData);
                ChunkReader chunkReader = new ChunkReaderWithFilter(chunk, filter);
                priorityMergeReader.addReaderWithPriority(new EngineChunkReader(chunkReader), priorityValue);
                priorityValue++;
            }
        }

        //TODO: add SeriesChunkReader in MemTable
        if (overflowSeriesDataSource.hasRawSeriesChunk()) {
            priorityMergeReader.addReaderWithPriority(new MemChunkReaderWithFilter(overflowSeriesDataSource.getRawSeriesChunk(), filter), priorityValue++);
        }

        //Add External Sort
//    timeValuePairReaders = externalSortJobEngine.execute(timeValuePairReaders);

        return new UnSeqSeriesReader(jobId, priorityMergeReader);
    }

//  public PriorityMergeReaderByTimestamp createSeriesReaderForOverflowInsertByTimestamp(OverflowSeriesDataSource overflowSeriesDataSource)
//          throws IOException {
//    long jobId = queryJobManager.addJobForOneQuery();
//    List<EncodedSeriesChunkDescriptor> seriesChunkDescriptorList =
//            SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowSeriesDataSource.getOverflowInsertFileList());
//    int priorityValue = 1;
//    List<PrioritySeriesReaderByTimestamp> timeValuePairReaders = new ArrayList<>();
//    for (EncodedSeriesChunkDescriptor seriesChunkDescriptor : seriesChunkDescriptorList) {
//      SeriesChunk seriesChunk = overflowSeriesChunkLoader.getChunk(jobId, seriesChunkDescriptor);
//      SeriesReaderByTimeStamp seriesChunkReader = new SeriesChunkReaderByTimestampImpl(seriesChunk.getSeriesChunkBodyStream(),
//              seriesChunkDescriptor.getDataType(), seriesChunkDescriptor.getCompressionTypeName());
//      PrioritySeriesReaderByTimestamp priorityTimeValuePairReader = new PrioritySeriesReaderByTimestamp(seriesChunkReader,
//              new PrioritySeriesReader.Priority(priorityValue));
//      timeValuePairReaders.add(priorityTimeValuePairReader);
//      priorityValue++;
//
//    }
//    //Add SeriesChunkReader in MemTable
//    if (overflowSeriesDataSource.hasRawSeriesChunk()) {
//      timeValuePairReaders.add(new PrioritySeriesReaderByTimestamp(new MemChunkReaderByTimestamp(
//              overflowSeriesDataSource.getRawSeriesChunk()), new PrioritySeriesReader.Priority(priorityValue++)));
//    }
//
//    return new PriorityMergeReaderByTimestamp(timeValuePairReaders);
//  }

    private boolean chunkSatisfied(ChunkMetaData metaData, Filter filter) {

        DigestForFilter digest = new DigestForFilter(
                metaData.getStartTime(),
                metaData.getEndTime(),
                metaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                metaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                metaData.getTsDataType());

        return filter.satisfy(digest);
    }

    public UnSeqSeriesReader createSeriesReaderForUnSeq(OverflowSeriesDataSource overflowSeriesDataSource) throws IOException {
        long jobId = queryJobManager.addJobForOneQuery();

        PriorityMergeReader priorityMergeReader = new PriorityMergeReader();

        int priorityValue = 1;

        for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource.getOverflowInsertFileList()) {
            TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(overflowInsertFile.getFilePath(), false);
            ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);

            for (ChunkMetaData chunkMetaData : overflowInsertFile.getChunkMetaDataList()) {
                Chunk chunk = chunkLoader.getChunk(chunkMetaData);
                ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
                priorityMergeReader.addReaderWithPriority(new EngineChunkReader(chunkReader), priorityValue);
                priorityValue++;
            }
        }


        //TODO: add SeriesChunkReader in MemTable
        if (overflowSeriesDataSource.hasRawSeriesChunk()) {
            priorityMergeReader.addReaderWithPriority(new MemChunkReaderWithoutFilter(overflowSeriesDataSource.getRawSeriesChunk()), priorityValue++);
        }

//    priorityMergeReader = externalSortJobEngine.execute(priorityMergeReader);

        return new UnSeqSeriesReader(jobId, priorityMergeReader);
    }

    public IReader createSeriesReaderForMerge(
            IntervalFileNode intervalFileNode, OverflowSeriesDataSource overflowSeriesDataSource, SingleSeriesExpression seriesFilter)
            throws IOException {
        logger.debug("create seriesReaders for merge. SeriesFilter = {}. TsFilePath = {}", seriesFilter, intervalFileNode.getFilePath());
        IReader seriesInTsFileReader = genSealedTsFileSeriesReader(intervalFileNode.getFilePath(), seriesFilter);

        PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
        IReader overflowInsertDataReader = createSeriesReaderForUnSeq(overflowSeriesDataSource, seriesFilter.getFilter());
        priorityMergeReader.addReaderWithPriority(seriesInTsFileReader, 1);
        priorityMergeReader.addReaderWithPriority(overflowInsertDataReader, 2);

        return priorityMergeReader;
    }

    private IReader genSealedTsFileSeriesReader(String filePath, SingleSeriesExpression seriesFilter) throws IOException {
        TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(filePath, true);
        ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);
        MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(tsFileSequenceReader);
        List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesFilter.getSeriesPath());
        FileSeriesReader seriesInTsFileReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, seriesFilter.getFilter());
        return new SealedTsFileReader(seriesInTsFileReader);
    }


    private static class SeriesReaderFactoryHelper {
        private static SeriesReaderFactory INSTANCE = new SeriesReaderFactory();
    }

    public static SeriesReaderFactory getInstance() {
        return SeriesReaderFactoryHelper.INSTANCE;
    }
}