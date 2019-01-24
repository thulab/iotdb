/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.overflow.ioV2.OverflowProcessor;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeManager implements IStatistic, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeManager.class);
  private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
  private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();
  private static final Directories directories = Directories.getInstance();
  private final String baseDir;
  /**
   * Stat information.
   */
  private final String statStorageDeltaName = MonitorConstants.statStorageGroupPrefix
      + MonitorConstants.MONITOR_PATH_SEPERATOR + MonitorConstants.fileNodeManagerPath;
  /**
   * This map is used to manage all filenode processor,<br> the key is filenode name which is
   * storage group seriesPath.
   */
  private ConcurrentHashMap<String, FileNodeProcessor> processorMap;
  /**
   * This set is used to store overflowed filenode name.<br> The overflowed filenode will be merge.
   */
  private volatile FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;
  // There is no need to add concurrently
  private HashMap<String, AtomicLong> statParamsHashMap = new HashMap<String, AtomicLong>() {
    {
      for (MonitorConstants.FileNodeManagerStatConstants fileNodeManagerStatConstant :
          MonitorConstants.FileNodeManagerStatConstants.values()) {
        put(fileNodeManagerStatConstant.name(), new AtomicLong(0));
      }
    }
  };

  private FileNodeManager(String baseDir) {
    processorMap = new ConcurrentHashMap<String, FileNodeProcessor>();

    if (baseDir.charAt(baseDir.length() - 1) != File.separatorChar) {
      baseDir += File.separatorChar;
    }
    this.baseDir = baseDir;
    File dir = new File(baseDir);
    if (dir.mkdirs()) {
      LOGGER.info("{} dir home doesn't exist, create it", dir.getPath());
    }

    // TsFileConf.duplicateIncompletedPage = true;
    if (TsFileDBConf.enableStatMonitor) {
      StatMonitor statMonitor = StatMonitor.getInstance();
      registStatMetadata();
      statMonitor.registStatistics(statStorageDeltaName, this);
    }
  }

  public static FileNodeManager getInstance() {
    return FileNodeManagerHolder.INSTANCE;
  }

  private void updateStatHashMapWhenFail(TSRecord tsRecord) {
    statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_REQ_FAIL.name())
        .incrementAndGet();
    statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS_FAIL.name())
        .addAndGet(tsRecord.dataPointList.size());
  }

  /**
   * get stats parameter hash map.
   *
   * @return the key represents the params' name, values is AtomicLong type
   */
  public HashMap<String, AtomicLong> getStatParamsHashMap() {
    return statParamsHashMap;
  }

  @Override
  public List<String> getAllPathForStatistic() {
    List<String> list = new ArrayList<>();
    for (MonitorConstants.FileNodeManagerStatConstants statConstant :
        MonitorConstants.FileNodeManagerStatConstants.values()) {
      list.add(
          statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name());
    }
    return list;
  }

  @Override
  public HashMap<String, TSRecord> getAllStatisticsValue() {
    long curTime = System.currentTimeMillis();
    TSRecord tsRecord = StatMonitor
        .convertToTSRecord(getStatParamsHashMap(), statStorageDeltaName, curTime);
    return new HashMap<String, TSRecord>() {
      {
        put(statStorageDeltaName, tsRecord);
      }
    };
  }

  /**
   * Init Stat MetaDta TODO: Modify the throws operation.
   */
  @Override
  public void registStatMetadata() {
    HashMap<String, String> hashMap = new HashMap<String, String>() {
      {
        for (MonitorConstants.FileNodeManagerStatConstants statConstant :
            MonitorConstants.FileNodeManagerStatConstants.values()) {
          put(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name(),
              MonitorConstants.DataType);
        }
      }
    };
    StatMonitor.getInstance().registStatStorageGroup(hashMap);
  }

  /**
   * This function is just for unit test.
   */
  public synchronized void resetFileNodeManager() {
    for (String key : statParamsHashMap.keySet()) {
      statParamsHashMap.put(key, new AtomicLong());
    }
    processorMap.clear();
  }

  private FileNodeProcessor constructNewProcessor(String filenodeName)
      throws FileNodeManagerException {
    try {
      return new FileNodeProcessor(baseDir, filenodeName);
    } catch (FileNodeProcessorException e) {
      LOGGER.error("Can't construct the FileNodeProcessor, the filenode is {}", filenodeName, e);
      throw new FileNodeManagerException(e);
    }
  }

  private FileNodeProcessor getProcessor(String path, boolean isWriteLock)
      throws FileNodeManagerException {
    String filenodeName;
    try {
      filenodeName = MManager.getInstance().getFileNameByPath(path);
    } catch (PathErrorException e) {
      LOGGER.error("MManager get filenode name error, seriesPath is {}", path);
      throw new FileNodeManagerException(e);
    }
    FileNodeProcessor processor = null;
    processor = processorMap.get(filenodeName);
    if (processor != null) {
      processor.lock(isWriteLock);
    } else {
      filenodeName = filenodeName.intern();
      // calculate the value with same key synchronously
      synchronized (filenodeName) {
        processor = processorMap.get(filenodeName);
        if (processor != null) {
          processor.lock(isWriteLock);
        } else {
          // calculate the value with the key monitor
          LOGGER.debug("Calcuate the processor, the filenode is {}, Thread is {}", filenodeName,
              Thread.currentThread().getId());
          processor = constructNewProcessor(filenodeName);
          processor.lock(isWriteLock);
          processorMap.put(filenodeName, processor);
        }
      }
    }
    // processorMap.putIfAbsent(seriesPath, processor);
    return processor;
  }

  /**
   * recovery the filenode processor.
   */
  public void recovery() {

    try {
      List<String> filenodeNames = MManager.getInstance().getAllFileNames();
      for (String filenodeName : filenodeNames) {
        FileNodeProcessor fileNodeProcessor = getProcessor(filenodeName, true);
        if (fileNodeProcessor.shouldRecovery()) {
          LOGGER.info("Recovery the filenode processor, the filenode is {}, the status is {}",
              filenodeName,
              fileNodeProcessor.getFileNodeProcessorStatus());
          fileNodeProcessor.fileNodeRecovery();
        } else {
          fileNodeProcessor.writeUnlock();
        }
        // add index check sum
        // fileNodeProcessor.rebuildIndex();
      }
    } catch (PathErrorException | FileNodeManagerException | FileNodeProcessorException e) {
      LOGGER.error("Restore all FileNode failed, the reason is {}", e.getMessage());
    }
  }

  /**
   * insert TsRecord into storage group.
   *
   * @param tsRecord input Data
   * @param isMonitor if true, the insertion is done by StatMonitor and the statistic Info will not
   * be recorded. if false, the statParamsHashMap will be updated.
   * @return an int value represents the insert type
   */
  public int insert(TSRecord tsRecord, boolean isMonitor) throws FileNodeManagerException {
    long timestamp = tsRecord.time;
    if (timestamp < 0) {
      LOGGER.error("The insert time lt 0, {}.", tsRecord);
      throw new FileNodeManagerException("The insert time lt 0, the tsrecord is " + tsRecord);
    }
    String deviceId = tsRecord.deviceId;

    if (!isMonitor) {
      statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS.name())
          .addAndGet(tsRecord.dataPointList.size());
    }

    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
    int insertType = 0;

    try {
      long lastUpdateTime = fileNodeProcessor.getFlushLastUpdateTime(deviceId);
      String filenodeName = fileNodeProcessor.getProcessorName();
      if (timestamp < lastUpdateTime) {
        // get overflow processor
        OverflowProcessor overflowProcessor;
        try {
          overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName);
        } catch (IOException e) {
          LOGGER.error("Get the overflow processor failed, the filenode is {}, insert time is {}",
              filenodeName, timestamp);
          if (!isMonitor) {
            updateStatHashMapWhenFail(tsRecord);
          }
          throw new FileNodeManagerException(e);
        }
        // write wal
        try {
          if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
            List<String> measurementList = new ArrayList<>();
            List<String> insertValues = new ArrayList<>();
            for (DataPoint dp : tsRecord.dataPointList) {
              measurementList.add(dp.getMeasurementId());
              insertValues.add(dp.getValue().toString());
            }
            overflowProcessor.getLogNode().write(
                new InsertPlan(2, tsRecord.deviceId, tsRecord.time, measurementList, insertValues));
          }
        } catch (IOException e) {
          if (!isMonitor) {
            updateStatHashMapWhenFail(tsRecord);
          }
          throw new FileNodeManagerException(e);
        }
        // write overflow data
        try {
          overflowProcessor.insert(tsRecord);
          fileNodeProcessor.changeTypeToChanged(deviceId, timestamp);
          fileNodeProcessor.setOverflowed(true);
          // if (shouldMerge) {
          // LOGGER.info(
          // "The overflow file or metadata reaches the threshold,
          // merge the filenode processor {}",
          // filenodeName);
          // fileNodeProcessor.submitToMerge();
          // }
        } catch (IOException e) {
          LOGGER.error("Insert into overflow error, the reason is {}", e.getMessage());
          if (!isMonitor) {
            updateStatHashMapWhenFail(tsRecord);
          }
          throw new FileNodeManagerException(e);
        }
        // change the type of tsfile to overflowed

        insertType = 1;
      } else {
        // get bufferwrite processor
        BufferWriteProcessor bufferWriteProcessor;
        try {
          bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(filenodeName, timestamp);
        } catch (FileNodeProcessorException e) {
          LOGGER
              .error("Get the bufferwrite processor failed, the filenode is {}, insert time is {}",
                  filenodeName, timestamp);
          if (!isMonitor) {
            updateStatHashMapWhenFail(tsRecord);
          }
          throw new FileNodeManagerException(e);
        }
        // Add a new interval file to newfilelist
        if (bufferWriteProcessor.isNewProcessor()) {
          bufferWriteProcessor.setNewProcessor(false);
          String bufferwriteBaseDir = bufferWriteProcessor.getBaseDir();
          String bufferwriteRelativePath = bufferWriteProcessor.getFileRelativePath();
          try {
            fileNodeProcessor
                .addIntervalFileNode(timestamp, bufferwriteBaseDir, bufferwriteRelativePath);
          } catch (Exception e) {
            if (!isMonitor) {
              updateStatHashMapWhenFail(tsRecord);
            }
            throw new FileNodeManagerException(e);
          }
        }
        // write wal
        try {
          if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
            List<String> measurementList = new ArrayList<>();
            List<String> insertValues = new ArrayList<>();
            for (DataPoint dp : tsRecord.dataPointList) {
              measurementList.add(dp.getMeasurementId());
              insertValues.add(dp.getValue().toString());
            }
            bufferWriteProcessor.getLogNode().write(
                new InsertPlan(2, tsRecord.deviceId, tsRecord.time, measurementList, insertValues));
          }
        } catch (IOException e) {
          if (!isMonitor) {
            updateStatHashMapWhenFail(tsRecord);
          }
          throw new FileNodeManagerException(e);
        }
        // Write data
        fileNodeProcessor.setIntervalFileNodeStartTime(deviceId);
        fileNodeProcessor.setLastUpdateTime(deviceId, timestamp);
        try {
          bufferWriteProcessor.write(tsRecord);
        } catch (BufferWriteProcessorException e) {
          if (!isMonitor) {
            updateStatHashMapWhenFail(tsRecord);
          }
          throw new FileNodeManagerException(e);
        }
        insertType = 2;
        if (bufferWriteProcessor
            .getFileSize() > IoTDBDescriptor.getInstance()
            .getConfig().bufferwriteFileSizeThreshold) {
          LOGGER.info(
              "The filenode processor {} will close the bufferwrite processor, "
                  + "because the size[{}] of tsfile {} reaches the threshold {}",
              filenodeName, MemUtils.bytesCntToStr(bufferWriteProcessor.getFileSize()),
              bufferWriteProcessor.getFileName(), MemUtils.bytesCntToStr(
                  IoTDBDescriptor.getInstance().getConfig().bufferwriteFileSizeThreshold));
          fileNodeProcessor.closeBufferWrite();
        }
      }
    } catch (FileNodeProcessorException e) {
      LOGGER.error(String.format("Encounter an error when closing the buffer write processor %s.",
          fileNodeProcessor.getProcessorName()), e);
      throw new FileNodeManagerException(e);
    } finally {
      fileNodeProcessor.writeUnlock();
    }
    // Modify the insert
    if (!isMonitor) {
      fileNodeProcessor.getStatParamsHashMap()
          .get(MonitorConstants.FileNodeProcessorStatConstants.TOTAL_POINTS_SUCCESS.name())
          .addAndGet(tsRecord.dataPointList.size());
      fileNodeProcessor.getStatParamsHashMap()
          .get(MonitorConstants.FileNodeProcessorStatConstants.TOTAL_REQ_SUCCESS.name())
          .incrementAndGet();
      statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_REQ_SUCCESS.name())
          .incrementAndGet();
      statParamsHashMap
          .get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS_SUCCESS.name())
          .addAndGet(tsRecord.dataPointList.size());
    }
    return insertType;
  }

  /**
   * update data.
   */
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String v)
      throws FileNodeManagerException {

    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
    try {

      long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deviceId);
      if (startTime > lastUpdateTime) {
        LOGGER.warn("The update range is error, startTime {} is great than lastUpdateTime {}",
            startTime,
            lastUpdateTime);
        return;
      }
      if (endTime > lastUpdateTime) {
        endTime = lastUpdateTime;
      }
      String filenodeName = fileNodeProcessor.getProcessorName();
      // get overflow processor
      OverflowProcessor overflowProcessor;
      try {
        overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName);
      } catch (IOException e) {
        LOGGER.error(
            "Get the overflow processor failed, the filenode is {}, "
                + "insert time range is from {} to {}",
            filenodeName, startTime, endTime);
        throw new FileNodeManagerException(e);
      }
      overflowProcessor.update(deviceId, measurementId, startTime, endTime, type, v);
      // change the type of tsfile to overflowed
      fileNodeProcessor.changeTypeToChanged(deviceId, startTime, endTime);
      fileNodeProcessor.setOverflowed(true);

      // write wal
      try {
        if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
          overflowProcessor.getLogNode()
              .write(
                  new UpdatePlan(startTime, endTime, v, new Path(deviceId + "." + measurementId)));
        }
      } catch (IOException e) {
        throw new FileNodeManagerException(e);
      }
      // if (shouldMerge) {
      // LOGGER.info("The overflow file or metadata reaches the
      // threshold, merge the filenode processor {}",
      // filenodeName);
      // fileNodeProcessor.submitToMerge();
      // }
    } finally {
      fileNodeProcessor.writeUnlock();
    }
  }

  /**
   * delete data.
   */
  public void delete(String deviceId, String measurementId, long timestamp, TSDataType type)
      throws FileNodeManagerException {

    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
    try {
      long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deviceId);
      // no tsfile data, the delete operation is invalid
      if (lastUpdateTime == -1) {
        LOGGER.warn(
            "The last update time is -1, delete overflow is invalid, the filenode processor is {}",
            fileNodeProcessor.getProcessorName());
      } else {
        if (timestamp > lastUpdateTime) {
          timestamp = lastUpdateTime;
        }
        String filenodeName = fileNodeProcessor.getProcessorName();
        // get overflow processor
        OverflowProcessor overflowProcessor;
        try {
          overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName);
        } catch (IOException e) {
          LOGGER.error("Get the overflow processor failed, the filenode is {}, delete time is {}.",
              filenodeName, timestamp);
          throw new FileNodeManagerException(e);
        }
        overflowProcessor.delete(deviceId, measurementId, timestamp, type);
        // change the type of tsfile to overflowed
        fileNodeProcessor.changeTypeToChangedForDelete(deviceId, timestamp);
        fileNodeProcessor.setOverflowed(true);
        // if (shouldMerge) {
        // LOGGER.info(
        // "The overflow file or metadata reaches the threshold,
        // merge the filenode processor {}",
        // filenodeName);
        // fileNodeProcessor.submitToMerge();
        // }
        fileNodeProcessor.changeTypeToChangedForDelete(deviceId, timestamp);
        fileNodeProcessor.setOverflowed(true);

        // write wal
        try {
          if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
            overflowProcessor.getLogNode()
                .write(new DeletePlan(timestamp, new Path(deviceId + "." + measurementId)));
          }
        } catch (IOException e) {
          throw new FileNodeManagerException(e);
        }
      }
    } finally {
      fileNodeProcessor.writeUnlock();
    }
  }

  /**
   * try to delete the filenode processor.
   */
  private void delete(String processorName,
      Iterator<Map.Entry<String, FileNodeProcessor>> processorIterator)
      throws FileNodeManagerException {
    if (processorMap.containsKey(processorName)) {
      LOGGER.info("Try to delete the filenode processor {}.", processorName);
      FileNodeProcessor processor = processorMap.get(processorName);
      if (processor.tryWriteLock()) {
        try {
          if (processor.canBeClosed()) {
            try {
              LOGGER.info("Delete the filenode processor {}.", processorName);
              processor.delete();
              processorIterator.remove();
            } catch (ProcessorException e) {
              LOGGER.error("Delete the filenode processor {} error.", processorName, e);
              throw new FileNodeManagerException(e);
            }
          } else {
            LOGGER.warn("The filenode processor {} can't be deleted.", processorName);
          }
        } finally {
          processor.writeUnlock();
        }
      } else {
        LOGGER.warn("Can't get the write lock of the filenode processor {}.", processorName);
      }
    } else {
      LOGGER.warn("The processorMap doesn't contain the filenode processor {}.", processorName);
    }
  }

  /**
   * begin query.
   */
  public int beginQuery(String deviceId) throws FileNodeManagerException {
    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
    try {
      LOGGER.debug("Get the FileNodeProcessor: filenode is {}, begin query.",
          fileNodeProcessor.getProcessorName());
      int token = fileNodeProcessor.addMultiPassLock();
      return token;
    } finally {
      fileNodeProcessor.writeUnlock();
    }
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression)
      throws FileNodeManagerException {
    String deviceId = seriesExpression.getSeriesPath().getDevice();
    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, false);
    LOGGER.debug("Get the FileNodeProcessor: filenode is {}, query.",
        fileNodeProcessor.getProcessorName());
    try {
      QueryDataSource queryDataSource = null;
      // query operation must have overflow processor
      if (!fileNodeProcessor.hasOverflowProcessor()) {
        try {
          fileNodeProcessor.getOverflowProcessor(fileNodeProcessor.getProcessorName());
        } catch (IOException e) {
          LOGGER.error("Get the overflow processor failed, the filenode is {}, query is {},{}",
              fileNodeProcessor.getProcessorName(), deviceId, measurementId);
          throw new FileNodeManagerException(e);
        }
      }
      try {
        queryDataSource = fileNodeProcessor
            .query(deviceId, measurementId, seriesExpression.getFilter());
      } catch (FileNodeProcessorException e) {
        LOGGER.error("Query error: the deviceId {}, the measurementId {}", deviceId, measurementId,
            e);
        throw new FileNodeManagerException(e);
      }
      // return query structure
      return queryDataSource;
    } finally {
      fileNodeProcessor.readUnlock();
    }
  }

  /**
   * end query.
   */
  public void endQuery(String deviceId, int token) throws FileNodeManagerException {

    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
    try {
      LOGGER.debug("Get the FileNodeProcessor: {}, filenode is {}, end query.",
          fileNodeProcessor.getProcessorName());
      fileNodeProcessor.removeMultiPassLock(token);
    } finally {
      fileNodeProcessor.writeUnlock();
    }
  }

  /**
   * Append one specified tsfile to the storage group. <b>This method is only provided for
   * transmission module</b>
   *
   * @param fileNodeName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public boolean appendFileToFileNode(String fileNodeName, IntervalFileNode appendFile,
      String appendFilePath)
      throws FileNodeManagerException {
    FileNodeProcessor fileNodeProcessor = getProcessor(fileNodeName, true);
    try {
      // check append file
      for (Map.Entry<String, Long> entry : appendFile.getStartTimeMap().entrySet()) {
        if (fileNodeProcessor.getLastUpdateTime(entry.getKey()) >= entry.getValue()) {
          return false;
        }
      }
      // close bufferwrite file
      fileNodeProcessor.closeBufferWrite();
      // append file to storage group.
      fileNodeProcessor.appendFile(appendFile, appendFilePath);
    } catch (FileNodeProcessorException e) {
      e.printStackTrace();
      throw new FileNodeManagerException(e);
    } finally {
      fileNodeProcessor.writeUnlock();
    }
    return true;
  }

  /**
   * get all overlap tsfiles which are conflict with the appendFile.
   *
   * @param fileNodeName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFilesFromFileNode(String fileNodeName, IntervalFileNode appendFile,
      String uuid)
      throws FileNodeManagerException {
    FileNodeProcessor fileNodeProcessor = getProcessor(fileNodeName, true);
    List<String> overlapFiles = new ArrayList<>();
    try {
      overlapFiles = fileNodeProcessor.getOverlapFiles(appendFile, uuid);
    } catch (FileNodeProcessorException e) {
      throw new FileNodeManagerException(e);
    } finally {
      fileNodeProcessor.writeUnlock();
    }
    return overlapFiles;
  }

  /**
   * merge all overflowed filenode.
   *
   * @throws FileNodeManagerException FileNodeManagerException
   */
  public void mergeAll() throws FileNodeManagerException {
    if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
      fileNodeManagerStatus = FileNodeManagerStatus.MERGE;
      LOGGER.info("Start to merge all overflowed filenode");
      List<String> allFileNodeNames;
      try {
        allFileNodeNames = MManager.getInstance().getAllFileNames();
      } catch (PathErrorException e) {
        LOGGER.error("Get all storage group seriesPath error,", e);
        e.printStackTrace();
        throw new FileNodeManagerException(e);
      }
      List<Future<?>> futureTasks = new ArrayList<>();
      for (String fileNodeName : allFileNodeNames) {
        FileNodeProcessor fileNodeProcessor = getProcessor(fileNodeName, true);
        try {
          Future<?> task = fileNodeProcessor.submitToMerge();
          if (task != null) {
            LOGGER.info("Submit the filenode {} to the merge pool", fileNodeName);
            futureTasks.add(task);
          }
        } finally {
          fileNodeProcessor.writeUnlock();
        }
      }
      long totalTime = 0;
      // loop waiting for merge to end, the longest waiting time is
      // 60s.
      int time = 2;
      for (Future<?> task : futureTasks) {
        while (!task.isDone()) {
          try {
            LOGGER.info(
                "Waiting for the end of merge, already waiting for {}s, "
                    + "continue to wait anothor {}s",
                totalTime, time);
            TimeUnit.SECONDS.sleep(time);
            totalTime += time;
            if (time < 32) {
              time = time * 2;
            } else {
              time = 60;
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      fileNodeManagerStatus = FileNodeManagerStatus.NONE;
      LOGGER.info("End to merge all overflowed filenode");
    } else {
      LOGGER.warn("Failed to merge all overflowed filenode, because filenode manager status is {}",
          fileNodeManagerStatus);
    }
  }

  /**
   * try to close the filenode processor. The name of filenode processor is processorName
   */
  private boolean closeOneProcessor(String processorName) throws FileNodeManagerException {
    if (processorMap.containsKey(processorName)) {
      Processor processor = processorMap.get(processorName);
      if (processor.tryWriteLock()) {
        try {
          if (processor.canBeClosed()) {
            processor.close();
            return true;
          } else {
            return false;
          }
        } catch (ProcessorException e) {
          LOGGER.error("Close the filenode processor {} error.", processorName, e);
          throw new FileNodeManagerException(e);
        } finally {
          processor.writeUnlock();
        }
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  /**
   * delete one filenode.
   */
  public boolean deleteOneFileNode(String processorName) throws FileNodeManagerException {
    if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
      fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
      try {
        if (processorMap.containsKey(processorName)) {
          LOGGER.info("Forced to delete the filenode processor {}", processorName);
          FileNodeProcessor processor = processorMap.get(processorName);
          while (true) {
            if (processor.tryWriteLock()) {
              try {
                if (processor.canBeClosed()) {
                  LOGGER.info("Delete the filenode processor {}.", processorName);
                  processor.delete();
                  processorMap.remove(processorName);
                  break;
                } else {
                  LOGGER.info(
                      "Can't delete the filenode processor {}, "
                          + "because the filenode processor can't be closed. Wait 100ms to retry");
                }
              } catch (ProcessorException e) {
                LOGGER.error("Delete the filenode processor {} error.", processorName, e);
                throw new FileNodeManagerException(e);
              } finally {
                processor.writeUnlock();
              }
            } else {
              LOGGER.info(
                  "Can't delete the filenode processor {}, because it can't get the write lock."
                      + " Wait 100ms to retry");
            }
            try {
              TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
              LOGGER.error(e.getMessage());
            }
          }
        }
        String fileNodePath = TsFileDBConf.fileNodeDir;
        fileNodePath = standardizeDir(fileNodePath) + processorName;
        FileUtils.deleteDirectory(new File(fileNodePath));

        List<String> bufferwritePathList = directories.getAllTsFileFolders();
        for (String bufferwritePath : bufferwritePathList) {
          bufferwritePath = standardizeDir(bufferwritePath) + processorName;
          File bufferDir = new File(bufferwritePath);
          // free and close the streams under this bufferwrite directory
          if (bufferDir.exists()) {
            for (File bufferFile : bufferDir.listFiles()) {
              FileReaderManager.getInstance().closeFileAndRemoveReader(bufferFile.getPath());
            }
          }
          FileUtils.deleteDirectory(new File(bufferwritePath));
        }

        String overflowPath = TsFileDBConf.overflowDataDir;
        overflowPath = standardizeDir(overflowPath) + processorName;
        File overflowDir = new File(overflowPath);
        if (overflowDir.exists()) {
          for (File subOverflowDir : overflowDir.listFiles()) {
            for (File overflowFile : subOverflowDir.listFiles()) {
              FileReaderManager.getInstance().closeFileAndRemoveReader(overflowFile.getPath());
            }
          }
        }
        FileUtils.deleteDirectory(new File(overflowPath));

        MultiFileLogNodeManager.getInstance()
            .deleteNode(processorName + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX);
        MultiFileLogNodeManager.getInstance()
            .deleteNode(processorName + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX);
        return true;
      } catch (IOException e) {
        LOGGER.error("Delete the filenode processor {} error.", processorName, e);
        throw new FileNodeManagerException(e);
      } finally {
        fileNodeManagerStatus = FileNodeManagerStatus.NONE;
      }
    } else {
      return false;
    }
  }

  private String standardizeDir(String originalPath) {
    String res = originalPath;
    if ((originalPath.length() > 0
        && originalPath.charAt(originalPath.length() - 1) != File.separatorChar)
        || originalPath.length() == 0) {
      res = originalPath + File.separatorChar;
    }
    return res;
  }

  /**
   * add time series.
   */
  public void addTimeSeries(Path path, String dataType, String encoding, String[] encodingArgs)
      throws FileNodeManagerException {
    FileNodeProcessor fileNodeProcessor = getProcessor(path.getFullPath(), true);
    try {
      fileNodeProcessor.addTimeSeries(path.getMeasurement(), dataType, encoding, encodingArgs);
    } finally {
      fileNodeProcessor.writeUnlock();
    }
  }

  /**
   * Force to close the filenode processor.
   */
  public void closeOneFileNode(String processorName) throws FileNodeManagerException {
    if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
      fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
      try {
        LOGGER.info("Force to close the filenode processor {}.", processorName);
        while (!closeOneProcessor(processorName)) {
          try {
            LOGGER.info("Can't force to close the filenode processor {}, wait 100ms to retry");
            TimeUnit.MILLISECONDS.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } finally {
        fileNodeManagerStatus = FileNodeManagerStatus.NONE;
      }
    }
  }

  /**
   * try to close the filenode processor.
   */
  private void close(String processorName) throws FileNodeManagerException {
    if (processorMap.containsKey(processorName)) {
      LOGGER.info("Try to close the filenode processor {}.", processorName);
      FileNodeProcessor processor = processorMap.get(processorName);
      if (processor.tryWriteLock()) {
        try {
          if (processor.canBeClosed()) {
            try {
              LOGGER.info("Close the filenode processor {}.", processorName);
              processor.close();
            } catch (ProcessorException e) {
              LOGGER.error("Close the filenode processor {} error.", processorName, e);
              throw new FileNodeManagerException(e);
            }
          } else {
            LOGGER.warn("The filenode processor {} can't be closed.", processorName);
          }
        } finally {
          processor.writeUnlock();
        }
      } else {
        LOGGER.warn("Can't get the write lock of the filenode processor {}.", processorName);
      }
    } else {
      LOGGER.warn("The processorMap doesn't contain the filenode processor {}.", processorName);
    }
  }

  /**
   * delete all filenode.
   */
  public synchronized boolean deleteAll() throws FileNodeManagerException {
    LOGGER.info("Start deleting all filenode");
    if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
      fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
      try {
        Iterator<Map.Entry<String, FileNodeProcessor>> processorIterator = processorMap.entrySet()
            .iterator();
        while (processorIterator.hasNext()) {
          Map.Entry<String, FileNodeProcessor> processorEntry = processorIterator.next();
          try {
            delete(processorEntry.getKey(), processorIterator);
          } catch (FileNodeManagerException e) {
            throw e;
          }
        }
        return processorMap.isEmpty();
      } catch (FileNodeManagerException e) {
        throw new FileNodeManagerException(e);
      } finally {
        LOGGER.info("Delete all filenode processor successfully");
        fileNodeManagerStatus = FileNodeManagerStatus.NONE;
      }
    } else {
      LOGGER.info("Failed to delete all filenode processor because of merge operation");
      return false;
    }
  }

  /**
   * Try to close All.
   */
  public void closeAll() throws FileNodeManagerException {
    LOGGER.info("Start closing all filenode processor");
    if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
      fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
      try {
        Iterator<Map.Entry<String, FileNodeProcessor>> processorIterator = processorMap.entrySet()
            .iterator();
        while (processorIterator.hasNext()) {
          Map.Entry<String, FileNodeProcessor> processorEntry = processorIterator.next();
          try {
            close(processorEntry.getKey());
          } catch (FileNodeManagerException e) {
            throw e;
          }
        }
      } catch (FileNodeManagerException e) {
        throw new FileNodeManagerException(e);
      } finally {
        LOGGER.info("Close all filenode processor successfully");
        fileNodeManagerStatus = FileNodeManagerStatus.NONE;
      }
    } else {
      LOGGER.info("Failed to close all filenode processor because of merge operation");
    }
  }

  /**
   * force flush to control memory usage.
   */
  public void forceFlush(BasicMemController.UsageLevel level) {
    // TODO : for each FileNodeProcessor, call its forceFlush()
    // you may add some delicate process like below
    // or you could provide multiple methods for different urgency
    switch (level) {
      case WARNING:
        // only select the most urgent (most active or biggest in size)
        // processors to flush
        // only select top 10% active memory user to flush
        try {
          flushTop(0.1f);
        } catch (IOException e) {
          LOGGER.error("force flush memory data error: {}", e.getMessage());
          e.printStackTrace();
        }
        break;
      case DANGEROUS:
        // force all processors to flush
        try {
          flushAll();
        } catch (IOException e) {
          LOGGER.error("force flush memory data error: {}", e.getMessage());
          e.printStackTrace();
        }
        break;
      case SAFE:
        // if the flush thread pool is not full ( or half full), start a new
        // flush task
        if (FlushManager.getInstance().getActiveCnt() < 0.5 * FlushManager.getInstance()
            .getThreadCnt()) {
          try {
            flushTop(0.01f);
          } catch (IOException e) {
            LOGGER.error("force flush memory data error:{}", e.getMessage());
            e.printStackTrace();
          }
        }
        break;
      default:
    }
  }

  private void flushAll() throws IOException {
    for (FileNodeProcessor processor : processorMap.values()) {
      if (processor.tryLock(true)) {
        try {
          boolean isMerge = processor.flush();
          if (isMerge) {
            processor.submitToMerge();
          }
        } finally {
          processor.unlock(true);
        }
      }
    }
  }

  private void flushTop(float percentage) throws IOException {
    List<FileNodeProcessor> tempProcessors = new ArrayList<>(processorMap.values());
    // sort the tempProcessors as descending order
    Collections.sort(tempProcessors, new Comparator<FileNodeProcessor>() {
      @Override
      public int compare(FileNodeProcessor o1, FileNodeProcessor o2) {
        return (int) (o2.memoryUsage() - o1.memoryUsage());
      }
    });
    int flushNum =
        (int) (tempProcessors.size() * percentage) > 1 ? (int) (tempProcessors.size() * percentage)
            : 1;
    for (int i = 0; i < flushNum && i < tempProcessors.size(); i++) {
      FileNodeProcessor processor = tempProcessors.get(i);
      // 64M
      if (processor.memoryUsage() > TsFileConf.groupSizeInByte / 2) {
        processor.writeLock();
        try {
          boolean isMerge = processor.flush();
          if (isMerge) {
            processor.submitToMerge();
          }
        } finally {
          processor.writeUnlock();
        }
      }
    }
  }

  @Override
  public void start() throws StartupException {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() {
    try {
      closeAll();
    } catch (FileNodeManagerException e) {
      LOGGER.error("Failed to close file node manager because {}.", e.getMessage());
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_NODE_SERVICE;
  }

  /**
   * get restore file path.
   */
  public String getRestoreFilePath(String processorName) {
    FileNodeProcessor fileNodeProcessor = processorMap.get(processorName);
    if (fileNodeProcessor != null) {
      return fileNodeProcessor.getFileNodeRestoreFilePath();
    } else {
      return null;
    }
  }

  /**
   * recover filenode.
   */
  public void recoverFileNode(String filenodeName)
      throws FileNodeProcessorException, FileNodeManagerException {
    FileNodeProcessor fileNodeProcessor = getProcessor(filenodeName, true);
    LOGGER
        .info("Recovery the filenode processor, the filenode is {}, the status is {}", filenodeName,
            fileNodeProcessor.getFileNodeProcessorStatus());
    fileNodeProcessor.fileNodeRecovery();
    // add index check sum
    // fileNodeProcessor.rebuildIndex();
  }

  private enum FileNodeManagerStatus {
    NONE, MERGE, CLOSE;
  }

  private static class FileNodeManagerHolder {

    private static FileNodeManager INSTANCE = new FileNodeManager(TsFileDBConf.fileNodeDir);
  }
}