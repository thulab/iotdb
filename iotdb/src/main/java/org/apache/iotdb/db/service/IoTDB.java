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
package org.apache.iotdb.db.service;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.builder.ExceptionBuilder;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.postback.receiver.ServerManager;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.manager.WriteLogNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IoTDBMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private final String MBEAN_NAME = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE,
      IoTDBConstant.JMX_TYPE, "IoTDB");
  private RegisterManager registerManager = new RegisterManager();
  private ServerManager serverManager = ServerManager.getInstance();

  public static final IoTDB getInstance() {
    return IoTDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    IoTDB daemon = IoTDB.getInstance();
    daemon.active();
  }

  public void active() {
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      checks.verify();
    } catch (StartupException e) {
      // TODO: what are some checks
      LOGGER.error("{}: failed to start because some checks failed. {}",
          IoTDBConstant.GLOBAL_DB_NAME, e.getMessage());
      return;
    }
    try {
      setUp();
    } catch (StartupException e) {
      LOGGER.error(e.getMessage());
      deactivate();
      LOGGER.error("{} exit", IoTDBConstant.GLOBAL_DB_NAME);
      return;
    }
    LOGGER.info("{} has started.", IoTDBConstant.GLOBAL_DB_NAME);
  }

  private void setUp() throws StartupException {
    setUncaughtExceptionHandler();

    FileNodeManager.getInstance().recovery();
    try {
      systemDataRecovery();
    } catch (RecoverException e) {
      String errorMessage = String.format("Failed to recover system data because of %s",
          e.getMessage());
      throw new StartupException(errorMessage);
    }
    // When registering statMonitor, we should start recovering some statistics
    // with latest values stored
    // Warn: registMonitor() method should be called after systemDataRecovery()
    if (IoTDBDescriptor.getInstance().getConfig().enableStatMonitor) {
      StatMonitor.getInstance().recovery();
    }

    registerManager.register(FileNodeManager.getInstance());
    registerManager.register(MultiFileLogNodeManager.getInstance());
    registerManager.register(JMXService.getInstance());
    registerManager.register(JDBCService.getInstance());
    registerManager.register(Monitor.INSTANCE);
    registerManager.register(CloseMergeService.getInstance());
    registerManager.register(StatMonitor.getInstance());
    registerManager.register(BasicMemController.getInstance());
    registerManager.register(FileReaderManager.getInstance());

    JMXService.registerMBean(getInstance(), MBEAN_NAME);

    initErrorInformation();

    serverManager.startServer();
  }

  public void deactivate() {
    serverManager.closeServer();
    registerManager.deregisterAll();
    JMXService.deregisterMBean(MBEAN_NAME);
  }

  @Override
  public void stop() {
    deactivate();
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private void initErrorInformation() {
    ExceptionBuilder.getInstance().loadInfo();
  }

  /**
   * Recover data using system log.
   *
   * @throws RecoverException if FileNode(Manager)Exception is encountered during the recovery.
   * @throws IOException      if IOException is encountered during the recovery.
   */
  private void systemDataRecovery() throws RecoverException {
    LOGGER.info("{}: start checking write log...", IoTDBConstant.GLOBAL_DB_NAME);

    WriteLogNodeManager writeLogManager = MultiFileLogNodeManager.getInstance();
    List<String> filenodeNames = null;
    try {
      filenodeNames = MManager.getInstance().getAllFileNames();
    } catch (PathErrorException e) {
      throw new RecoverException(e);
    }
    for (String filenodeName : filenodeNames) {
      if (writeLogManager.hasWAL(filenodeName)) {
        try {
          FileNodeManager.getInstance().recoverFileNode(filenodeName);
        } catch (FileNodeProcessorException | FileNodeManagerException e) {
          throw new RecoverException(e);
        }
      }
    }
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    boolean enableWal = config.enableWal;
    config.enableWal = false;
    writeLogManager.recover();
    config.enableWal = enableWal;
  }

  private static class IoTDBHolder {

    private static final IoTDB INSTANCE = new IoTDB();
  }

}
