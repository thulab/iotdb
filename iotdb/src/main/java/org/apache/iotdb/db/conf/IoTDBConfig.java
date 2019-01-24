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
package org.apache.iotdb.db.conf;

import java.io.File;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.MManager;

public class IoTDBConfig {

  public static final String CONFIG_NAME = "iotdb-engine.properties";
  public static final String default_data_dir = "data";
  public static final String default_sys_dir = "system";
  public static final String default_tsfile_dir = "settled";
  public static final String mult_dir_strategy_prefix =
      "org.apache.iotdb.db.conf.directories.strategy.";
  public static final String default_mult_dir_strategy = "MaxDiskUsableSpaceFirstStrategy";

  /**
   * Port which the JDBC server listens to.
   */
  public int rpcPort = 6667;

  /**
   * Is the write ahead log enable.
   */
  public boolean enableWal = false;

  /**
   * When a certain amount of write ahead logs is reached, they will be flushed to the disk. It is
   * possible to lose at most flush_wal_threshold operations.
   */
  public int flushWalThreshold = 10000;

  /**
   * The cycle when write ahead logs are periodically refreshed to disk(in milliseconds). It is
   * possible to lose at most flush_wal_period_in_ms ms operations.
   */
  public long flushWalPeriodInMs = 10000;

  /**
   * The cycle when write ahead log is periodically forced to be written to disk(in milliseconds) If
   * set this parameter to 0 it means call outputStream.force(true) after every each write
   */
  public long forceWalPeriodInMs = 10;

  /**
   * Data directory.
   */
  public String dataDir = null;

  /**
   * System directory.
   */
  public String sysDir = null;

  /**
   * Wal directory.
   */
  public String walDir = null;

  /**
   * Data directory of Overflow data.
   */
  public String overflowDataDir = "overflow";

  /**
   * Data directory of fileNode data.
   */
  public String fileNodeDir = "info";

  /**
   * Data directory of bufferWrite data.
   */
  // public String[] bufferWriteDirs = {"settled1", "settled2", "settled3"};
  public String[] bufferWriteDirs = {"settled"};

  /**
   * Strategy of multiple directories.
   */
  public String multDirStrategyClassName = null;

  /**
   * Data directory of metadata data.
   */
  public String metadataDir = "schema";

  /**
   * Data directory of derby data.
   */
  public String derbyHome = "derby";

  /**
   * Data directory of Write ahead log folder.
   */
  public String walFolder = "wal";

  /**
   * Data directory for index files (KV-match indexes).
   */
  public String indexFileDir = "index";

  /**
   * Temporary directory for temporary files of read (External Sort). TODO: unused field
   */
  public String readTmpFileDir = "readTmp";

  /**
   * The maximum concurrent thread number for merging overflow. When the value <=0 or > CPU core
   * number, use the CPU core number.
   */
  public int mergeConcurrentThreads = Runtime.getRuntime().availableProcessors();

  /**
   * Maximum number of folders open at the same time.
   */
  public int maxOpenFolder = 100;

  /**
   * The amount of data that is read every time when IoTDB merges data.
   */
  public int fetchSize = 10000;

  /**
   * the maximum number of writing instances existing in same time.
   */
  @Deprecated
  public int writeInstanceThreshold = 5;

  /**
   * The period time of flushing data from memory to file. . The unit is second.
   */
  public long periodTimeForFlush = 3600;

  /**
   * The period time for merge overflow data with tsfile data. The unit is second.
   */
  public long periodTimeForMerge = 7200;

  /**
   * When set true, start timed flush and merge service. Else, stop timed flush and merge service.
   * The default value is true. TODO: 'timed' better explains this than 'timing'.
   */
  public boolean enableTimingCloseAndMerge = true;

  /**
   * How many threads can concurrently flush. When <= 0, use CPU core number.
   */
  public int concurrentFlushThread = Runtime.getRuntime().availableProcessors();

  public ZoneId zoneID = ZoneId.systemDefault();
  /**
   * BufferWriteProcessor and OverflowProcessor will immediately flush if this threshold is
   * reached.
   */
  public long memThresholdWarning = (long) (0.5 * Runtime.getRuntime().maxMemory());
  /**
   * No more insert is allowed if this threshold is reached.
   */
  public long memThresholdDangerous = (long) (0.6 * Runtime.getRuntime().maxMemory());
  /**
   * MemMonitorThread will check every such interval(in ms). If memThresholdWarning is reached,
   * MemMonitorThread will inform FileNodeManager to flush.
   */
  public long memMonitorInterval = 1000;
  /**
   * Decide how to control memory usage of inserting data. 0 is RecordMemController, which sums the
   * size of each record (tuple). 1 is JVMMemController, which uses the JVM heap memory as the
   * memory usage indicator.
   */
  public int memControllerType = 1;
  /**
   * When a bufferwrite's metadata size (in byte) exceed this, the bufferwrite is forced closed.
   */
  public long bufferwriteMetaSizeThreshold = 200 * 1024 * 1024L;
  /**
   * When a bufferwrite's file size (in byte) exceed this, the bufferwrite is forced closed.
   */
  public long bufferwriteFileSizeThreshold = 2 * 1024 * 1024 * 1024L;
  /**
   * When a overflow's metadata size (in byte) exceed this, the overflow is forced closed.
   */
  public long overflowMetaSizeThreshold = 20 * 1024 * 1024L;
  /**
   * When a overflow's file size (in byte) exceed this, the overflow is forced closed.
   */
  public long overflowFileSizeThreshold = 200 * 1024 * 1024L;
  /**
   * If set false, MemMonitorThread and MemStatisticThread will not be created.
   */
  public boolean enableMemMonitor = false;
  /**
   * When set to true, small flushes will be triggered periodically even if the memory threshold is
   * not exceeded.
   */
  public boolean enableSmallFlush = false;
  /**
   * The interval of small flush in ms.
   */
  public long smallFlushInterval = 60 * 1000;
  /**
   * The statMonitor writes statistics info into IoTDB every backLoopPeriodSec secs. The default
   * value is 5s.
   */
  public int backLoopPeriodSec = 5;
  /**
   * Set true to enable statistics monitor service, false to disable statistics service.
   */
  public boolean enableStatMonitor = false;
  /**
   * Set the time interval when StatMonitor performs delete detection. The default value is 600s.
   */
  public int statMonitorDetectFreqSec = 60 * 10;
  /**
   * Set the maximum time to keep monitor statistics information in IoTDB. The default value is
   * 600s.
   */
  public int statMonitorRetainIntervalSec = 60 * 10;
  /**
   * Threshold for external sort. When using multi-line merging sort, if the count of lines exceed
   * {@code externalSortThreshold}, it will trigger external sort.
   */
  public int externalSortThreshold = 50;
  /**
   * Cache size of {@code checkAndGetDataTypeCache} in {@link MManager}.
   */
  public int mManagerCacheSize = 400000;
  /**
   * The maximum size of a single log in byte. If a log exceeds this size, it cannot be written to
   * the WAL file and an exception is thrown.
   */
  public int maxLogEntrySize = 4 * 1024 * 1024;
  /**
   * Is this IoTDB instance a receiver of postback or not.
   */
  public boolean isPostbackEnable = true;
  /**
   * If this IoTDB instance is a receiver of postback, set the server port.
   */
  public int postbackServerPort = 5555;
  /*
   * Set the language version when loading file including error information, default value is "EN"
   */
  public String languageVersion = "EN";
  /**
   * Choose a postBack strategy of merging historical data: 1. It's more likely to update historical
   * data, choose "true". 2. It's more likely not to update historical data or you don't know
   * exactly, choose "false".
   */
  public boolean update_historical_data_possibility = false;
  public String ipWhiteList = "0.0.0.0/0";
  /**
   * Examining period of cache file reader : 100 seconds.
   */
  public long cacheFileReaderClearPeriod = 100000;

  public IoTDBConfig() {
  }

  public ZoneId getZoneID() {
    return zoneID;
  }

  public String getZoneIDString() {
    return zoneID.toString();
  }

  public void updatePath() {
    confirmMultDirStrategy();

    preUpdatePath();

    // update the paths of subdirectories in the dataDir
    if (dataDir.length() > 0 && !dataDir.endsWith(File.separator)) {
      dataDir = dataDir + File.separatorChar;
    }
    overflowDataDir = dataDir + overflowDataDir;

    if (bufferWriteDirs == null || bufferWriteDirs.length == 0) {
      bufferWriteDirs = new String[]{default_tsfile_dir};
    }
    for (int i = 0; i < bufferWriteDirs.length; i++) {
      if (new File(bufferWriteDirs[i]).isAbsolute()) {
        continue;
      }

      bufferWriteDirs[i] = dataDir + bufferWriteDirs[i];
    }

    // update the paths of subdirectories in the sysDir
    if (sysDir.length() > 0 && !sysDir.endsWith(File.separator)) {
      sysDir = sysDir + File.separatorChar;
    }
    fileNodeDir = sysDir + fileNodeDir;
    metadataDir = sysDir + metadataDir;

    // update the paths of subdirectories in the walDir
    if (walDir.length() > 0 && !walDir.endsWith(File.separator)) {
      walDir = walDir + File.separatorChar;
    }
    walFolder = walDir + walFolder;

    derbyHome = sysDir + derbyHome;
    indexFileDir = dataDir + indexFileDir;
  }

  /*
   * First, if dataDir is null, dataDir will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"data".
   * Then, if dataDir is absolute, leave dataDir as it is. If dataDir is relative, dataDir
   * will be converted to the complete version using non-empty %IOTDB_HOME%. e.g.
   * for windows platform, | IOTDB_HOME | dataDir before | dataDir
   * after | |-----------------|--------------------|---------------------------| |
   * D:\\iotdb\iotdb | null |
   * D:\\iotdb\iotdb\data\data | | D:\\iotdb\iotdb | dataDir | D:\\iotdb\iotdb\dataDir |
   * | D:\\iotdb\iotdb |
   * C:\\dataDir | C:\\dataDir | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   * First, if sysDir is null, sysDir will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"system".
   * Then, if sysDir is absolute, leave sysDir as it is. If sysDir is relative,
   * sysDir will be converted to the complete version using non-empty %IOTDB_HOME%.
   * e.g. for windows platform, | IOTDB_HOME | sysDir before | sysDir
   * after | |-----------------|--------------------|-----------------------------|
   * | D:\\iotdb\iotdb | null |D:\\iotdb\iotdb\data\system | | D:\\iotdb\iotdb | sysDir
   * | D:\\iotdb\iotdb\sysDir | | D:\\iotdb\iotdb |
   * C:\\sysDir | C:\\sysDir | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   * First, if walDir is null, walDir will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"data". Then,
   * if walDir is absolute, leave walDir as it is. If walDir is relative,
   * walDir will be converted to the complete
   * version using non-empty %IOTDB_HOME%. e.g. for windows platform,
   * | IOTDB_HOME | walDir before | walDir after |
   * |-----------------|--------------------|-----------------------------|
   * | D:\\iotdb\iotdb | null |
   * D:\\iotdb\iotdb\data\wal | | D:\\iotdb\iotdb | walDir | D:\\iotdb\iotdb\walDir |
   * | D:\\iotdb\iotdb | C:\\walDir |
   * C:\\walDir | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   */

  public void preUpdatePath() {
    if (dataDir == null) {
      dataDir = default_data_dir + File.separatorChar + default_data_dir;
    }
    if (sysDir == null) {
      sysDir = default_data_dir + File.separatorChar + default_sys_dir;
    }
    if (walDir == null) {
      walDir = default_data_dir;
    }

    List<String> dirs = new ArrayList<>();
    dirs.add(dataDir);
    dirs.add(sysDir);
    dirs.add(walDir);
    // List<String> newdirs = new ArrayList<>();
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    for (int i = 0; i < 3; i++) {
      String dir = dirs.get(i);
      if (new File(dir).isAbsolute()) {
        continue;
      } else {
        if (homeDir != null) {
          if (homeDir.length() > 0) {
            if (!homeDir.endsWith(File.separator)) {
              dir = homeDir + File.separatorChar + dir;
            } else {
              dir = homeDir + dir;
            }
            dirs.set(i, dir);
          }
        }
      }
    }
    dataDir = dirs.get(0);
    sysDir = dirs.get(1);
    walDir = dirs.get(2);
  }

  public void confirmMultDirStrategy() {
    if (multDirStrategyClassName == null) {
      multDirStrategyClassName = default_mult_dir_strategy;
    }
    if (!multDirStrategyClassName.contains(".")) {
      multDirStrategyClassName = mult_dir_strategy_prefix + multDirStrategyClassName;
    }

    try {
      Class.forName(multDirStrategyClassName);
    } catch (ClassNotFoundException e) {
      multDirStrategyClassName = mult_dir_strategy_prefix + default_mult_dir_strategy;
    }
  }

  public String[] getBufferWriteDirs() {
    return bufferWriteDirs;
  }
}
