package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.reader.mem.MemChunkReaderWithFilter;
import cn.edu.tsinghua.iotdb.queryV2.reader.mem.MemChunkReaderWithoutFilter;
import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.queryV2.reader.sequence.SealedTsFileReader;
import cn.edu.tsinghua.iotdb.queryV2.reader.unsequence.EngineChunkReader;
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
import java.util.List;


public class SeriesReaderFactory {

    private static final Logger logger = LoggerFactory.getLogger(SeriesReaderFactory.class);
//    private OverflowSeriesChunkLoader overflowSeriesChunkLoader;
    //  private ExternalSortJobEngine externalSortJobEngine;
//    private QueryJobManager queryJobManager;

    private SeriesReaderFactory() {
//        overflowSeriesChunkLoader = new OverflowSeriesChunkLoader();
//    externalSortJobEngine = SimpleExternalSortEngine.getInstance();
//        queryJobManager = QueryJobManager.getInstance();
    }

    public PriorityMergeReader createUnSeqMergeReader(OverflowSeriesDataSource overflowSeriesDataSource, Filter filter)
            throws IOException {

//        long jobId = queryJobManager.addJobForOneQuery();

        PriorityMergeReader unSeqMergeReader = new PriorityMergeReader();

        int priorityValue = 1;

        for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource.getOverflowInsertFileList()) {
            TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(overflowInsertFile.getFilePath(), false);
            ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);

            for (ChunkMetaData chunkMetaData : overflowInsertFile.getChunkMetaDataList()) {
                Chunk chunk = chunkLoader.getChunk(chunkMetaData);
                ChunkReader chunkReader;

                if (filter != null)
                    chunkReader = new ChunkReaderWithFilter(chunk, filter);
                else
                    chunkReader = new ChunkReaderWithoutFilter(chunk);

                unSeqMergeReader.addReaderWithPriority(new EngineChunkReader(chunkReader), priorityValue);
                priorityValue++;
            }
        }

        // add reader for MemTable
        if (overflowSeriesDataSource.hasRawChunk()) {
            if (filter != null)
                unSeqMergeReader.addReaderWithPriority(new MemChunkReaderWithFilter(overflowSeriesDataSource.getRawChunk(), filter), priorityValue);
            else
                unSeqMergeReader.addReaderWithPriority(new MemChunkReaderWithoutFilter(overflowSeriesDataSource.getRawChunk()), priorityValue);
        }

        // TODO add External Sort
        // timeValuePairReaders = externalSortJobEngine.executeWithGlobalTimeFilter(timeValuePairReaders);

        return unSeqMergeReader;
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
//      EngineSeriesReaderByTimeStamp seriesChunkReader = new SeriesChunkReaderByTimestampImpl(seriesChunk.getSeriesChunkBodyStream(),
//              seriesChunkDescriptor.getDataType(), seriesChunkDescriptor.getCompressionTypeName());
//      PrioritySeriesReaderByTimestamp priorityTimeValuePairReader = new PrioritySeriesReaderByTimestamp(seriesChunkReader,
//              new PrioritySeriesReader.Priority(priorityValue));
//      timeValuePairReaders.add(priorityTimeValuePairReader);
//      priorityValue++;
//
//    }
//    //Add SeriesChunkReader in MemTable
//    if (overflowSeriesDataSource.hasRawChunk()) {
//      timeValuePairReaders.add(new PrioritySeriesReaderByTimestamp(new MemChunkReaderByTimestamp(
//              overflowSeriesDataSource.getRawChunk()), new PrioritySeriesReader.Priority(priorityValue++)));
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

    /**
     * This method is used to construct reader for merge process in IoTDB.
     * To merge only one TsFile data and one UnSeqFile data.
     */
    public IReader createSeriesReaderForMerge(
            IntervalFileNode intervalFileNode, OverflowSeriesDataSource overflowSeriesDataSource, SingleSeriesExpression singleSeriesExpression)
            throws IOException {

        logger.debug("create seriesReaders for merge. SeriesFilter = {}. TsFilePath = {}",
                singleSeriesExpression, intervalFileNode.getFilePath());

        PriorityMergeReader priorityMergeReader = new PriorityMergeReader();

        // Sequence reader
        IReader seriesInTsFileReader = genSealedTsFileSeriesReader(intervalFileNode.getFilePath(), singleSeriesExpression);
        priorityMergeReader.addReaderWithPriority(seriesInTsFileReader, 1);

        // unSequence merge reader
        IReader unSeqMergeReader = createUnSeqMergeReader(overflowSeriesDataSource, singleSeriesExpression.getFilter());
        priorityMergeReader.addReaderWithPriority(unSeqMergeReader, 2);


        return priorityMergeReader;
    }

    private IReader genSealedTsFileSeriesReader(String filePath, SingleSeriesExpression singleSeriesExpression) throws IOException {
        TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(filePath, true);
        ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);
        MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(tsFileSequenceReader);
        List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(singleSeriesExpression.getSeriesPath());

        // TODO is singleSeriesExpression.getFilter() always null?
        FileSeriesReader seriesInTsFileReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, singleSeriesExpression.getFilter());
        return new SealedTsFileReader(seriesInTsFileReader);
    }


    private static class SeriesReaderFactoryHelper {
        private static SeriesReaderFactory INSTANCE = new SeriesReaderFactory();
    }

    public static SeriesReaderFactory getInstance() {
        return SeriesReaderFactoryHelper.INSTANCE;
    }
}