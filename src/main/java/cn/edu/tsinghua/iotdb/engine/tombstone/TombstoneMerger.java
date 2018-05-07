package cn.edu.tsinghua.iotdb.engine.tombstone;

import cn.edu.tsinghua.iotdb.engine.cache.RowGroupBlockMetaDataCache;
import cn.edu.tsinghua.iotdb.engine.cache.TsFileMetaDataCache;
import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.filenode.OverflowChangeType;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.TombstoneMergeException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TombstoneMerger {
    private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneMerger.class);
    private static final String TEMP_SUFFIX = ".temp";
    public static final String COMPLETE_SUFFIX = ".complete";
    private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
    private static final long DELETE_FILE_TRY_INTERVAL = 10000;
    private static final long DELETE_FILE_TRY_TIMEOUT = 600000;

    private IntervalFileNode tsFile;
    private TombstoneFile tombstoneFile;
    private FileSchema fileSchema;

    public TombstoneMerger(IntervalFileNode tsFile, FileSchema fileSchema) throws IOException {
        this.tsFile = tsFile;
        this.tombstoneFile = tsFile.getTombstoneFile();
        this.fileSchema = fileSchema;
    }

    public void merge() throws TombstoneMergeException {
        tombstoneFile.lock();
        try {
            TsFileWriter recordWriter = null;
            File tempFile = new File(tsFile.getFilePath() + TEMP_SUFFIX);
            tempFile.delete();
            Collection<String> deltaObjects = tsFile.listDeltaObjects();
            List<Tombstone> tombstones;
            try {
                tombstones = tombstoneFile.getTombstones();
            } catch (IOException e) {
                throw new TombstoneMergeException(e);
            }
            try {
                tombstoneFile.close();
            } catch (IOException e) {
                LOGGER.error("Cannot close tombstone file when merging {}, because {}", tsFile.getFilePath(), e.getMessage());
            }

            List<Tombstone> deltaObjectTombstones = new ArrayList<>();
            List<Tombstone> seriesTombstones = new ArrayList<>();
            for (String deltaObjectId : deltaObjects) {
                // query one deltaObjectId
                List<Path> pathList = new ArrayList<>();
                try {
                    List<String> pathStrings = MManager.getInstance().getLeafNodePathInNextLevel(deltaObjectId);
                    for (String string : pathStrings) {
                        pathList.add(new Path(string));
                    }
                } catch (PathErrorException e) {
                    LOGGER.error("Can't get all the paths from MManager, the deltaObjectId is {}", deltaObjectId);
                    throw new TombstoneMergeException(e);
                }
                if (pathList.isEmpty()) {
                    continue;
                }
                // query tombstones of this deltaObject
                deltaObjectTombstones.clear();
                for (Tombstone tombstone : tombstones) {
                    if (tombstone.deltaObjectId.equals(deltaObjectId))
                        deltaObjectTombstones.add(tombstone);
                }
                // query one measurenment in the special deltaObjectId
                Filter<Long> timeFilter = FilterFactory.and(
                        TimeFilter.gtEq(tsFile.getStartTime(deltaObjectId)),
                        TimeFilter.ltEq(tsFile.getEndTime(deltaObjectId)));
                long startTime;
                long endTime;
                Map<String, Long> startTimeMap = new HashMap<>();
                Map<String, Long> endTimeMap = new HashMap<>();
                for (Path path : pathList) {
                    int dataCnt = 0;
                    // query tombstones of this series
                    seriesTombstones.clear();
                    for (Tombstone tombstone : deltaObjectTombstones) {
                        if (tombstone.measurementId.equals(path.getMeasurementToString()))
                            seriesTombstones.add(tombstone);
                    }
                    // construct series reader
                    SeriesFilter<Long> seriesFilter = new SeriesFilter<>(path, timeFilter);
                    SeriesReader seriesReader;
                    try {
                        seriesReader = SeriesReaderFactory.getInstance().genTsFileSeriesReader(tsFile.getFilePath(), seriesFilter, seriesTombstones);
                    } catch (IOException e) {
                        throw new TombstoneMergeException(e);
                    }
                    // read and write data
                    try {
                        if (!seriesReader.hasNext()) {
                            LOGGER.debug("The time-series {} has no data with the filter {} when merge with tombstone",
                                    path, seriesFilter);
                        } else {
                            TimeValuePair timeValuePair = seriesReader.next();
                            if (recordWriter == null) {
                                recordWriter = new TsFileWriter(tempFile, fileSchema, TsFileConf);
                            }
                            TSRecord record = constructTsRecord(timeValuePair, deltaObjectId,
                                    path.getMeasurementToString());
                            recordWriter.write(record);
                            dataCnt ++;
                            startTime = endTime = timeValuePair.getTimestamp();
                            if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                                startTimeMap.put(deltaObjectId, startTime);
                            }
                            if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                                endTimeMap.put(deltaObjectId, endTime);
                            }
                            while (seriesReader.hasNext()) {
                                record = constructTsRecord(seriesReader.next(), deltaObjectId,
                                        path.getMeasurementToString());
                                endTime = record.time;
                                recordWriter.write(record);
                                dataCnt ++;
                            }
                            if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                                endTimeMap.put(deltaObjectId, endTime);
                            }
                        }
                    } catch (IOException | WriteProcessException e) {
                        throw new TombstoneMergeException(e);
                    } finally {
                        try {
                            seriesReader.close();
                        } catch (IOException e) {
                            LOGGER.error("Cannot close series reader when merging {}, because {}", tsFile.getFilePath(), e.getMessage());
                        }
                    }
                    LOGGER.debug("Write out a series {} in {} size {}", path, tsFile.getFilePath(), dataCnt);
                }
                tsFile.setStartTimeMap(startTimeMap);
                tsFile.setEndTimeMap(endTimeMap);
            }
            TsFileMetaDataCache.getInstance().remove(tsFile.getFilePath());
            RowGroupBlockMetaDataCache.getInstance().removeFile(tsFile.getFilePath());
            if (recordWriter != null) {
                try {
                    recordWriter.close();
                    LOGGER.debug("New file written, file size {}", tempFile.length());
                } catch (IOException e) {
                    LOGGER.error("Cannot close record writer after tombstone merge of {}", tsFile.getFilePath());
                }
            }
            // mark the temp file as completed
            File completeFile = new File(tempFile.getPath().replace(TEMP_SUFFIX, COMPLETE_SUFFIX));
            completeFile.delete();
            if(!tempFile.renameTo(completeFile)) {
                LOGGER.error("Cannot rename temp file to complete file when merging tombstones of {}", tsFile.getFilePath());
            } else {
                File tsF = new File(tsFile.getFilePath());
                // the old file is deleted by overflow merge or else, just delete the temp file
                if(!tsF.exists()) {
                    LOGGER.debug("Old file {} has been deleted, abort merge", tsF.getPath());
                    completeFile.delete();
                } else {
                    // if the tsfile is not in use, replace it with the new file and delete tombstone file
                    // or wait until the tsfile is not in use or timeout is reached
                    long startTime = System.currentTimeMillis();
                    while(System.currentTimeMillis() - startTime < DELETE_FILE_TRY_TIMEOUT) {
                        if(tsF.delete()) {
                            completeFile.renameTo(tsF);
                            try {
                                if(!tombstoneFile.delete())
                                    LOGGER.error("Cannot delete tombstone file when merging tombstones of {}", tsFile.getFilePath());
                            } catch (IOException e) {
                                LOGGER.error("Cannot delete tombstone file when merging tombstones of {}, because ", tsFile.getFilePath(), e);
                            }
                            LOGGER.debug("Replace old file succeeded");
                            return;
                        } else if(!tsF.exists()) {
                            // this file might be used in overflow merge, and deleted after that, so the new file becomes useless
                            LOGGER.debug("Old file {} has been deleted, abort merge", tsF.getPath());
                            completeFile.delete();
                            return;
                        }
                        try {
                            Thread.sleep(DELETE_FILE_TRY_INTERVAL);
                        } catch (InterruptedException e) {
                            LOGGER.error("interrupted when waiting to delete the old file");
                            completeFile.delete();
                            return;
                        }
                    }
                    LOGGER.error("Timeout reached when trying to delete old tsfile after merging tombstones of {}", tsFile.getFilePath());
                    completeFile.delete();
                    // or leave the new file until next merge finds that the old file is no longer in use
                }
            }
        } finally {
            tombstoneFile.unlock();
        }
    }


    private TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }
}
