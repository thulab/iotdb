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
package org.apache.iotdb.db.engine.memcontrol;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicMemController implements IService {

  private static final Logger logger = LoggerFactory.getLogger(BasicMemController.class);
  protected long warningThreshold;
  protected long dangerouseThreshold;
  protected MemMonitorThread monitorThread;
  protected MemStatisticThread memStatisticThread;
  private IoTDBConfig config;

  BasicMemController(IoTDBConfig config) {
    this.config = config;
    warningThreshold = config.memThresholdWarning;
    dangerouseThreshold = config.memThresholdDangerous;
  }

  /**
   * change instance here.
   *
   * @return BasicMemController
   */
  public static BasicMemController getInstance() {
    switch (ControllerType.values()[IoTDBDescriptor.getInstance().getConfig().memControllerType]) {
      case JVM:
        return JVMMemController.getInstance();
      case RECORD:
      default:
        return RecordMemController.getInstance();
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      if (config.enableMemMonitor) {
        if (monitorThread == null) {
          monitorThread = new MemMonitorThread(config);
          monitorThread.start();
        } else {
          logger.error("Attempt to start MemController but it has already started");
        }
        if (memStatisticThread == null) {
          memStatisticThread = new MemStatisticThread();
          memStatisticThread.start();
        } else {
          logger.warn("Attempt to start MemController but it has already started");
        }
      }
      logger.info("MemController starts");
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage);
    }

  }

  @Override
  public void stop() {
    clear();
    close();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.JVM_MEM_CONTROL_SERVICE;
  }

  /**
   * set dangerous threshold.
   *
   * @param dangerouseThreshold dangerous threshold
   */
  public void setDangerousThreshold(long dangerouseThreshold) {
    this.dangerouseThreshold = dangerouseThreshold;
  }

  /**
   * set warning threshold.
   *
   * @param warningThreshold warning threshold
   */
  public void setWarningThreshold(long warningThreshold) {
    this.warningThreshold = warningThreshold;
  }

  /**
   * set check interval.
   *
   * @param checkInterval check interval
   */
  public void setCheckInterval(long checkInterval) {
    if (this.monitorThread != null) {
      this.monitorThread.setCheckInterval(checkInterval);
    }
  }

  public abstract long getTotalUsage();

  public abstract UsageLevel getCurrLevel();

  public abstract void clear();

  /**
   * close MemController.
   */
  public void close() {
    logger.info("MemController exiting");
    if (monitorThread != null) {
      monitorThread.interrupt();
      while (monitorThread.isAlive()) {
      }
      monitorThread = null;
    }

    if (memStatisticThread != null) {
      memStatisticThread.interrupt();
      while (memStatisticThread.isAlive()) {
      }
      memStatisticThread = null;
    }
    logger.info("MemController exited");
  }

  public abstract UsageLevel reportUse(Object user, long usage);

  public abstract void reportFree(Object user, long freeSize);

  public enum ControllerType {
    RECORD, JVM
  }

  public enum UsageLevel {
    SAFE, WARNING, DANGEROUS
  }
}
