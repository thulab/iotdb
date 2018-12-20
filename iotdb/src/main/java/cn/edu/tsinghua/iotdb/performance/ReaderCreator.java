//package cn.edu.tsinghua.iotdb.performance;
//
//import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
//import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
//import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowIO;
//import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
//import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
//import cn.edu.tsinghua.iotdb.query.factory.SeriesReaderFactory;
//import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
//import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
//import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
//import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
//import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
//import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
//import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
//import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
//import cn.edu.tsinghua.tsfile.read.common.Path;
//import cn.edu.tsinghua.tsfile.read.expression.impl.SeriesFilter;
//import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
//import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
//import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
//import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.IReader;
//import cn.edu.tsinghua.tsfile.write.io.TsFileIOWriter;
//import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.ByteArrayInputStream;
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//
//public class ReaderCreator {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderCreator.class);
//
//    private static final int FOOTER_LENGTH = 4;
//    private static final int POS_LENGTH = 8;
//    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
//
//    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> unSeqFileMetaData;
//
//    /**
//     * Create a reader to merge one series.
//     *
//     * @param tsfilePath
//     * @param unseqTsFilePath
//     * @param seriesPath the seriesPath of series to be merge.
//     * @return IReader
//     * @throws IOException
//     */
//    public static IReader createReaderForMerge(String tsfilePath, String unseqTsFilePath,
//                                                           Path seriesPath, long startTime, long endTime) throws IOException {
//        OverflowSeriesDataSource overflowSeriesDataSource = genDataSource(unseqTsFilePath, seriesPath);
//        TsfileDBDescriptor.getInstance().getConfig().bufferWriteDirs = new String[] {""};
//        Filter<?> filter = FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
//        SeriesFilter<?> seriesFilter = new SeriesFilter<>(seriesPath, filter);
//        IntervalFileNode intervalFileNode = new IntervalFileNode(null, tsfilePath);
//        IReader reader = SeriesReaderFactory.getInstance().createSeriesReaderForMerge(intervalFileNode, overflowSeriesDataSource, seriesFilter);
//        return reader;
//    }
//
//
//    public static IReader createReaderOnlyForOverflowInsert(String unseqTsFilePath,
//                                                                        Path seriesPath, long startTime, long endTime) throws IOException {
//        OverflowSeriesDataSource overflowSeriesDataSource = genDataSource(unseqTsFilePath, seriesPath);
//        TsfileDBDescriptor.getInstance().getConfig().bufferWriteDirs = new String[] {""};
//        Filter<?> filter = FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
//        SeriesFilter<?> seriesFilter = new SeriesFilter<>(seriesPath, filter);
//        IReader reader = SeriesReaderFactory.getInstance().createUnSeqMergeReader(overflowSeriesDataSource,
//                filter);
//        return reader;
//    }
//
//    /**
//     * Get the metadata of given file seriesPath.
//     *
//     * @param randomAccessFileReader
//     * @return
//     * @throws IOException
//     */
//    static TsFileMetaData getTsFileMetadata(ITsRandomAccessFileReader randomAccessFileReader) throws IOException {
//        long l = randomAccessFileReader.length();
//        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
//        int fileMetaDataLength = randomAccessFileReader.readInt();
//        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
//        byte[] buf = new byte[fileMetaDataLength];
//        randomAccessFileReader.read(buf, 0, buf.length);
//        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
//        return new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
//    }
//
//    static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> getUnSeqFileMetaData(String unseqTsfilePath) throws IOException {
//        File file = new File(unseqTsfilePath);
//        OverflowIO overflowIO = new OverflowIO(unseqTsfilePath, file.length(), true);
//        if (unSeqFileMetaData == null) {
//            unSeqFileMetaData = readMetadata(overflowIO);
//        }
//        return unSeqFileMetaData;
//    }
//
//    private static OverflowSeriesDataSource genDataSource(String unSeqTsFilePath, Path seriesPath) throws IOException {
//        if (unSeqFileMetaData == null) {
//            File file = new File(unSeqTsFilePath);
//            OverflowIO overflowIO = new OverflowIO(unSeqTsFilePath, file.length(), true);
//            unSeqFileMetaData = readMetadata(overflowIO);
//        }
//        OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(seriesPath);
//        OverflowInsertFile overflowInsertFile = new OverflowInsertFile();
//        overflowInsertFile.setPath(unSeqTsFilePath);
//        overflowInsertFile.setTimeSeriesChunkMetaData(unSeqFileMetaData.get(seriesPath.getDeltaObjectToString()).get(seriesPath.getMeasurementToString()));
//        List<OverflowInsertFile> overflowInsertFileList = new ArrayList<>();
//        overflowInsertFileList.add(overflowInsertFile);
//        overflowSeriesDataSource.setOverflowInsertFileList(overflowInsertFileList);
//        overflowSeriesDataSource.setReadableMemChunk(null);
//        return overflowSeriesDataSource;
//    }
//
//    /**
//     * Get the overflow metadata map of overflow insert data.
//     *
//     * @param insertIO
//     * @return
//     * @throws IOException
//     */
//    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> readMetadata(OverflowIO insertIO) throws IOException {
//
//        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> insertMetadatas = new HashMap<>();
//        // read insert meta-data
//        insertIO.toTail();
//        long position = insertIO.getPos();
//        while (position != 0) {
//            insertIO.getReader().seek(position - FOOTER_LENGTH);
//            int metadataLength = insertIO.getReader().readInt();
//            byte[] buf = new byte[metadataLength];
//            insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength);
//            insertIO.getReader().read(buf, 0, buf.length);
//            ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
//            RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
//                    .readRowGroupBlockMetaData(inputStream);
//            TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
//            blockMeta.convertToTSF(rowGroupBlockMetaData);
//            byte[] bytesPosition = new byte[8];
//            insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength - POS_LENGTH);
//            insertIO.getReader().read(bytesPosition, 0, POS_LENGTH);
//            position = BytesUtils.bytesToLong(bytesPosition);
//            for (RowGroupMetaData rowGroupMetaData : blockMeta.getRowGroups()) {
//                String deltaObjectId = rowGroupMetaData.getDeltaObjectID();
//                if (!insertMetadatas.containsKey(deltaObjectId)) {
//                    insertMetadatas.put(deltaObjectId, new HashMap<>());
//                }
//                for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
//                    String measurementId = chunkMetaData.getProperties().getMeasurementUID();
//                    if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
//                        insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
//                    }
//                    insertMetadatas.get(deltaObjectId).get(measurementId).add(0, chunkMetaData);
//                }
//            }
//        }
//        return insertMetadatas;
//    }
//}
