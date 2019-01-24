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
package org.apache.iotdb.db.engine.bufferwrite;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushUtil;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.utils.FlushStatus;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferWriteProcessor extends Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessor.class);
  RestorableTsFileIOWriter writer;
  // private RestorableTsFileIOWriter bufferWriteRestoreManager;
  private FileSchema fileSchema;
  private volatile FlushStatus flushStatus = new FlushStatus();
  private volatile boolean isFlush;
  private ReentrantLock flushQueryLock = new ReentrantLock();
  private AtomicLong memSize = new AtomicLong();
  private long memThreshold = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
  private IMemTable workMemTable;
  private IMemTable flushMemTable;
  private Action bufferwriteFlushAction;
  private Action bufferwriteCloseAction;
  private Action filenodeFlushAction;

  private long lastFlushTime = -1;
  private long valueCount = 0;

  private String baseDir;
  private String fileName;
  private String insertFilePath;
  private String bufferWriteRelativePath;

  private WriteLogNode logNode;

  /**
   * constructor of BufferWriteProcessor.
   *
   * @param baseDir base dir
   * @param processorName processor name
   * @param fileName file name
   * @param parameters parameters in Map(String, Action) structure
   * @param fileSchema file schema
   * @throws BufferWriteProcessorException BufferWriteProcessorException
   */
  public BufferWriteProcessor(String baseDir, String processorName, String fileName,
      Map<String, Action> parameters,
      FileSchema fileSchema) throws BufferWriteProcessorException {
    super(processorName);
    this.fileSchema = fileSchema;
    this.baseDir = baseDir;
    this.fileName = fileName;

    if (baseDir.length() > 0 && baseDir.charAt(baseDir.length() - 1) != File.separatorChar) {
      baseDir = baseDir + File.separatorChar;
    }
    String dataDirPath = baseDir + processorName;
    File dataDir = new File(dataDirPath);
    if (!dataDir.exists()) {
      dataDir.mkdirs();
      LOGGER.debug("The bufferwrite processor data dir doesn't exists, create new directory {}.",
          dataDirPath);
    }
    this.insertFilePath = new File(dataDir, fileName).getPath();
    bufferWriteRelativePath = processorName + File.separatorChar + fileName;
    try {
      writer = new RestorableTsFileIOWriter(processorName, insertFilePath);
    } catch (IOException e) {
      throw new BufferWriteProcessorException(e);
    }

    bufferwriteFlushAction = parameters.get(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
    bufferwriteCloseAction = parameters.get(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
    filenodeFlushAction = parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
    workMemTable = new PrimitiveMemTable();

    if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
      try {
        logNode = MultiFileLogNodeManager.getInstance().getNode(
            processorName + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX,
            getBufferwriteRestoreFilePath(),
            FileNodeManager.getInstance().getRestoreFilePath(processorName));
      } catch (IOException e) {
        throw new BufferWriteProcessorException(e);
      }
    }
  }

  /**
   * write one data point to the buffer write.
   *
   * @param deviceId device name
   * @param measurementId sensor name
   * @param timestamp timestamp of the data point
   * @param dataType the data type of the value
   * @param value data point value
   * @return true -the size of tsfile or metadata reaches to the threshold. false -otherwise
   * @throws BufferWriteProcessorException if a flushing operation occurs and failed.
   */
  public boolean write(String deviceId, String measurementId, long timestamp, TSDataType dataType,
      String value)
      throws BufferWriteProcessorException {
    TSRecord record = new TSRecord(timestamp, deviceId);
    DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, value);
    record.addTuple(dataPoint);
    return write(record);
  }

  /**
   * wrete a ts record into the memtable. If the memory usage is beyond the memThreshold, an async
   * flushing operation will be called.
   *
   * @param tsRecord data to be written
   * @return FIXME what is the mean about the return value??
   * @throws BufferWriteProcessorException if a flushing operation occurs and failed.
   */
  public boolean write(TSRecord tsRecord) throws BufferWriteProcessorException {
    long memUsage = MemUtils.getRecordSize(tsRecord);
    BasicMemController.UsageLevel level = BasicMemController.getInstance()
        .reportUse(this, memUsage);
    for (DataPoint dataPoint : tsRecord.dataPointList) {
      workMemTable.write(tsRecord.deviceId, dataPoint.getMeasurementId(), dataPoint.getType(),
          tsRecord.time,
          dataPoint.getValue().toString());
    }
    valueCount++;
    switch (level) {
      case SAFE:
        checkMemThreshold4Flush(memUsage);
        return true;
      case WARNING:
        LOGGER.warn("Memory usage will exceed warning threshold, current : {}.",
            MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
        checkMemThreshold4Flush(memUsage);
        return true;
      case DANGEROUS:
      default:
        LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}.",
            MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
        // FIXME if it is dangerous,
        // I think we need to reject comming insertions until the memory is safe.
        return false;
    }
  }

  private void checkMemThreshold4Flush(long addedMemory) throws BufferWriteProcessorException {
    addedMemory = memSize.addAndGet(addedMemory);
    if (addedMemory > memThreshold) {
      LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
          MemUtils.bytesCntToStr(addedMemory), getProcessorName(),
          MemUtils.bytesCntToStr(memThreshold));
      try {
        flush();
      } catch (IOException e) {
        e.printStackTrace();
        throw new BufferWriteProcessorException(e);
      }
    }
  }

  /**
   * get the one (or two) chunk(s) in the memtable ( and the other one in flushing status and then
   * compact them into one TimeValuePairSorter). Then get its (or their) ChunkMetadata(s).
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data and chunk metadata in memory
   */
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> queryBufferWriteData(String deviceId,
      String measurementId,
      TSDataType dataType) {
    flushQueryLock.lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      if (isFlush) {
        memSeriesLazyMerger.addMemSeries(flushMemTable.query(deviceId, measurementId, dataType));
      }
      memSeriesLazyMerger.addMemSeries(workMemTable.query(deviceId, measurementId, dataType));
      ReadOnlyMemChunk timeValuePairSorter = new ReadOnlyMemChunk(dataType, memSeriesLazyMerger);
      return new Pair<>(timeValuePairSorter,
          writer.getMetadatas(deviceId, measurementId, dataType));
    } finally {
      flushQueryLock.unlock();
    }
  }

  private void switchWorkToFlush() {
    flushQueryLock.lock();
    try {
      if (flushMemTable == null) {
        flushMemTable = workMemTable;
        workMemTable = new PrimitiveMemTable();
      }
    } finally {
      isFlush = true;
      flushQueryLock.unlock();
    }
  }

  private void switchFlushToWork() {
    flushQueryLock.lock();
    try {
      flushMemTable.clear();
      flushMemTable = null;
      writer.appendMetadata();
    } finally {
      isFlush = false;
      flushQueryLock.unlock();
    }
  }

  private void flushOperation(String flushFunction) {
    long flushStartTime = System.currentTimeMillis();
    LOGGER.info("The bufferwrite processor {} starts flushing {}.", getProcessorName(),
        flushFunction);
    try {
      if (flushMemTable != null && !flushMemTable.isEmpty()) {
        long startPos = writer.getPos();
        long startTime = System.currentTimeMillis();
        // flush data
        MemTableFlushUtil.flushMemTable(fileSchema, writer, flushMemTable);
        // write restore information
        writer.flush();
      }

      filenodeFlushAction.act();
      if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
        logNode.notifyEndFlush(null);
      }
    } catch (IOException e) {
      LOGGER.error("The bufferwrite processor {} failed to flush {}.", getProcessorName(),
          flushFunction, e);
    } catch (Exception e) {
      LOGGER.error(
          "The bufferwrite processor {} failed to flush {}, when calling the filenodeFlushAction.",
          getProcessorName(), flushFunction, e);
    } finally {
      synchronized (flushStatus) {
        flushStatus.setUnFlushing();
        switchFlushToWork();
        flushStatus.notify();
        LOGGER.info("The bufferwrite processor {} ends flushing {}.", getProcessorName(),
            flushFunction);
      }
    }
    long flushEndTime = System.currentTimeMillis();
    long flushInterval = flushEndTime - flushStartTime;
    ZonedDateTime startDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(flushStartTime),
        IoTDBDescriptor.getInstance().getConfig().getZoneID());
    ZonedDateTime endDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(flushEndTime),
        IoTDBDescriptor.getInstance().getConfig().getZoneID());
    LOGGER.info(
        "The bufferwrite processor {} flush {}, start time is {}, flush end time is {}, "
            + "flush time consumption is {}ms",
        getProcessorName(), flushFunction, startDateTime, endDateTime, flushInterval);
  }

  private Future<?> flush(boolean synchronization) throws IOException {
    // statistic information for flush
    if (lastFlushTime > 0) {
      long thisFlushTime = System.currentTimeMillis();
      long flushTimeInterval = thisFlushTime - lastFlushTime;
      ZonedDateTime lastDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastFlushTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      ZonedDateTime thisDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisFlushTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      LOGGER.info(
          "The bufferwrite processor {}: last flush time is {}, this flush time is {}, "
              + "flush time interval is {}s",
          getProcessorName(), lastDateTime, thisDateTime, flushTimeInterval / 1000);
    }
    lastFlushTime = System.currentTimeMillis();
    // check value count
    if (valueCount > 0) {
      // waiting for the end of last flush operation.
      synchronized (flushStatus) {
        while (flushStatus.isFlushing()) {
          try {
            flushStatus.wait();
          } catch (InterruptedException e) {
            LOGGER.error(
                "Encounter an interrupt error when waitting for the flushing, "
                    + "the bufferwrite processor is {}.",
                getProcessorName(), e);
          }
        }
      }
      // update the lastUpdatetime, prepare for flush
      try {
        bufferwriteFlushAction.act();
      } catch (Exception e) {
        LOGGER.error("Failed to flush bufferwrite row group when calling the action function.");
        throw new IOException(e);
      }
      if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
        logNode.notifyStartFlush();
      }
      valueCount = 0;
      flushStatus.setFlushing();
      switchWorkToFlush();
      BasicMemController.getInstance().reportFree(this, memSize.get());
      memSize.set(0);
      // switch
      if (synchronization) {
        flushOperation("synchronously");
      } else {
        FlushManager.getInstance().submit(() -> flushOperation("asynchronously"));
      }
    }
    // TODO return a meaningful Future
    return null;
  }

  @Override
  public boolean flush() throws IOException {
    flush(false);
    return false;
  }

  @Override
  public boolean canBeClosed() {
    return true;
  }

  @Override
  public void close() throws BufferWriteProcessorException {
    try {
      long closeStartTime = System.currentTimeMillis();
      // flush data
      flush(true);
      // end file
      writer.endFile(fileSchema);
      // update the IntervalFile for interval list
      bufferwriteCloseAction.act();
      // flush the changed information for filenode
      filenodeFlushAction.act();
      // delete the restore for this bufferwrite processor
      long closeEndTime = System.currentTimeMillis();
      long closeInterval = closeEndTime - closeStartTime;
      ZonedDateTime startDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(closeStartTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      ZonedDateTime endDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(closeEndTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      LOGGER.info(
          "Close bufferwrite processor {}, the file name is {}, start time is {}, end time is {}, "
              + "time consumption is {}ms",
          getProcessorName(), fileName, startDateTime, endDateTime, closeInterval);
    } catch (IOException e) {
      LOGGER.error("Close the bufferwrite processor error, the bufferwrite is {}.",
          getProcessorName(), e);
      throw new BufferWriteProcessorException(e);
    } catch (Exception e) {
      LOGGER
          .error("Failed to close the bufferwrite processor when calling the action function.", e);
      throw new BufferWriteProcessorException(e);
    }
  }

  @Override
  public long memoryUsage() {
    return memSize.get();
  }

  /**
   * check if is flushing.
   *
   * @return True if flushing
   */
  public boolean isFlush() {
    synchronized (flushStatus) {
      return flushStatus.isFlushing();
    }
  }

  /**
   * get metadata size.
   *
   * @return The sum of all timeseries's metadata size within this file.
   */
  public long getMetaSize() {
    // TODO : [MemControl] implement this
    return 0;
  }

  /**
   * get file size.
   *
   * @return The file size of the TsFile corresponding to this processor.
   */
  public long getFileSize() {
    // TODO : save this variable to avoid object creation?
    File file = new File(insertFilePath);
    return file.length() + memoryUsage();
  }

  /**
   * Close current TsFile and open a new one for future writes. Block new writes and wait until
   * current writes finish.
   */
  public void rollToNewFile() {
    // TODO : [MemControl] implement this
  }

  /**
   * Check if this TsFile has too big metadata or file. If true, close current file and open a new
   * one.
   */
  private boolean checkSize() throws IOException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long metaSize = getMetaSize();
    long fileSize = getFileSize();
    if (metaSize >= config.bufferwriteMetaSizeThreshold
        || fileSize >= config.bufferwriteFileSizeThreshold) {
      LOGGER.info(
          "The bufferwrite processor {}, size({}) of the file {} reaches threshold {}, "
              + "size({}) of metadata reaches threshold {}.",
          getProcessorName(), MemUtils.bytesCntToStr(fileSize), this.fileName,
          MemUtils.bytesCntToStr(config.bufferwriteFileSizeThreshold),
          MemUtils.bytesCntToStr(metaSize),
          MemUtils.bytesCntToStr(config.bufferwriteFileSizeThreshold));

      rollToNewFile();
      return true;
    }
    return false;
  }

  public String getBaseDir() {
    return baseDir;
  }

  public String getFileName() {
    return fileName;
  }

  public String getFileRelativePath() {
    return bufferWriteRelativePath;
  }

  private String getBufferwriteRestoreFilePath() {
    return writer.getRestoreFilePath();
  }

  public boolean isNewProcessor() {
    return writer.isNewResource();
  }

  public void setNewProcessor(boolean isNewProcessor) {
    writer.setNewResource(isNewProcessor);
  }

  public WriteLogNode getLogNode() {
    return logNode;
  }
}
