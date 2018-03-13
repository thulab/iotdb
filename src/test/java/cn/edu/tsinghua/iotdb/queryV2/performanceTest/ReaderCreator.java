package cn.edu.tsinghua.iotdb.queryV2.performanceTest;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowIO;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class ReaderCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderCreator.class);

    private static final int FOOTER_LENGTH = 4;
    private static final int POS_LENGTH = 8;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final String unseqTsFilePathName = "unseqTsFilec";
    private static final String restoreFilePathName = ".restore";
    private static final String fileFolder = "/D/data";

    /**
     * Create a reader to merge one series.
     *
     * @param tsfilePath
     * @param unseqTsFilePath
     * @param path the path of series to be merge.
     * @return TimeValuePairReader
     * @throws IOException
     */
    public static TimeValuePairReader createReaderForMerge(String tsfilePath, String unseqTsFilePath, Path path) throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(tsfilePath);
        TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);
        long startTime = tsFileMetaData.getDeltaObject(path.getDeltaObjectToString()).startTime;
        long endTime = tsFileMetaData.getDeltaObject(path.getDeltaObjectToString()).endTime;
        System.out.println(startTime + " - " + endTime);

        OverflowSeriesDataSource overflowSeriesDataSource = genDataSource(unseqTsFilePath, path);

        TsfileDBDescriptor.getInstance().getConfig().bufferWriteDir = "";
        Filter<?> filter = TimeFilter.gtEq(0L);
        SeriesFilter<?> seriesFilter = new SeriesFilter<>(path, filter);
        IntervalFileNode intervalFileNode = new IntervalFileNode(null, tsfilePath);
        System.out.println(intervalFileNode.getFilePath());
        TimeValuePairReader reader = SeriesReaderFactory.getInstance().createSeriesReaderForMerge(intervalFileNode, overflowSeriesDataSource, seriesFilter);
        return reader;
    }

    /**
     * Get the metadata of given file path.
     *
     * @param randomAccessFileReader
     * @return
     * @throws IOException
     */
    private static TsFileMetaData getTsFileMetadata(ITsRandomAccessFileReader randomAccessFileReader) throws IOException {
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);
        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        return new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
    }

    /**
     * Get all of the timeseries paths of given data folder.
     *
     * @param fileFolderName
     * @return
     */
    public static Set<Path> getAllPaths(String fileFolderName) throws IOException {
        Set<Path> pathSet = new HashSet<>();

        File dirFile = new File(fileFolderName);
        if (!dirFile.exists() || (!dirFile.isDirectory())) {
            LOGGER.error("the given folder name {} is wrong", fileFolderName);
            return pathSet;
        }

        File[] subFiles = dirFile.listFiles();
        if (subFiles == null || subFiles.length == 0) {
            LOGGER.error("the given folder {} has no overflow folder", fileFolderName);
            return pathSet;
        }

        for (File file : subFiles) {
            if (!file.getName().endsWith(restoreFilePathName)) {
                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);
                for (String deltaObjectID : tsFileMetaData.getDeltaObjectMap().keySet()) {
                    for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                        pathSet.add(new Path(deltaObjectID + "." + timeSeriesMetadata.getMeasurementUID()));
                    }
                }
                randomAccessFileReader.close();
            }
        }

        return pathSet;
    }

    public static Set<String> getAllMeasurements(String fileFolderName) throws IOException {
        Set<String> measurementSet = new HashSet<>();

        File dirFile = new File(fileFolderName);
        if (!dirFile.exists() || (!dirFile.isDirectory())) {
            LOGGER.error("the given folder name {} is wrong", fileFolderName);
            return measurementSet;
        }

        File[] subFiles = dirFile.listFiles();
        if (subFiles == null || subFiles.length == 0) {
            LOGGER.error("the given folder {} has no overflow folder", fileFolderName);
            return measurementSet;
        }

        for (File file : subFiles) {
            if (!file.getName().endsWith(restoreFilePathName)) {
                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);
                for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                    measurementSet.add(timeSeriesMetadata.getMeasurementUID());
                }
                randomAccessFileReader.close();
            }
        }

        return measurementSet;
    }

    private static OverflowSeriesDataSource genDataSource(String unseqTsfilePath, Path path) throws IOException {
        File file = new File(unseqTsfilePath);
        OverflowIO overflowIO = new OverflowIO(unseqTsfilePath, file.length(), true);
        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metadata = readMetadata(overflowIO);
        OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(path);
        OverflowInsertFile overflowInsertFile = new OverflowInsertFile();
        overflowInsertFile.setPath(unseqTsfilePath);
        overflowInsertFile.setTimeSeriesChunkMetaDatas(metadata.get(path.getDeltaObjectToString()).get(path.getMeasurementToString()));
        List<OverflowInsertFile> overflowInsertFileList = new ArrayList<>();
        overflowInsertFileList.add(overflowInsertFile);
        overflowSeriesDataSource.setOverflowInsertFileList(overflowInsertFileList);
        overflowSeriesDataSource.setRawSeriesChunk(null);
        return overflowSeriesDataSource;
    }

    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> readMetadata(OverflowIO insertIO) throws IOException {

        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> insertMetadatas = new HashMap<>();
        // read insert meta-data
        insertIO.toTail();
        long position = insertIO.getPos();
        while (position != 0) {
            insertIO.getReader().seek(position - FOOTER_LENGTH);
            int metadataLength = insertIO.getReader().readInt();
            byte[] buf = new byte[metadataLength];
            insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength);
            insertIO.getReader().read(buf, 0, buf.length);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
            RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
                    .readRowGroupBlockMetaData(inputStream);
            TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
            blockMeta.convertToTSF(rowGroupBlockMetaData);
            byte[] bytesPosition = new byte[8];
            insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength - POS_LENGTH);
            insertIO.getReader().read(bytesPosition, 0, POS_LENGTH);
            position = BytesUtils.bytesToLong(bytesPosition);
            for (RowGroupMetaData rowGroupMetaData : blockMeta.getRowGroups()) {
                String deltaObjectId = rowGroupMetaData.getDeltaObjectID();
                if (!insertMetadatas.containsKey(deltaObjectId)) {
                    insertMetadatas.put(deltaObjectId, new HashMap<>());
                }
                for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
                    String measurementId = chunkMetaData.getProperties().getMeasurementUID();
                    if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
                        insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
                    }
                    insertMetadatas.get(deltaObjectId).get(measurementId).add(0, chunkMetaData);
                }
            }
        }
        return insertMetadatas;
    }
}
