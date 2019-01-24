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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartupChecks {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartupChecks.class);
  public static final StartupCheck checkJMXPort = new StartupCheck() {

    @Override
    public void execute() throws StartupException {
      String jmxPort = System.getProperty(IoTDBConstant.REMOTE_JMX_PORT_NAME);
      if (jmxPort == null) {
        LOGGER.warn("JMX is not enabled to receive remote connection. "
                + "Please check conf/{}.sh(Unix or OS X, if you use Windows, "
                + "check conf/{}.bat) for more info",
            IoTDBConstant.ENV_FILE_NAME, IoTDBConstant.ENV_FILE_NAME);
        jmxPort = System.getProperty(IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
        if (jmxPort == null) {
          LOGGER.warn("{} missing from {}.sh(Unix or OS X, if you use Windows,"
                  + " check conf/{}.bat)",
              IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME, IoTDBConstant.ENV_FILE_NAME,
              IoTDBConstant.ENV_FILE_NAME);
        }
      } else {
        LOGGER.info("JMX is enabled to receive remote connection on port {}", jmxPort);
      }
    }
  };
  public static final StartupCheck checkJDK = new StartupCheck() {

    @Override
    public void execute() throws StartupException {
      int version = CommonUtils.getJdkVersion();
      if (version < IoTDBConstant.minSupportedJDKVerion) {
        throw new StartupException(
            String.format("Requires JDK version >= %d, current version is %d",
                IoTDBConstant.minSupportedJDKVerion, version));
      } else {
        LOGGER.info("JDK veriosn is {}.", version);
      }
    }
  };
  private final List<StartupCheck> preChecks = new ArrayList<>();
  private final List<StartupCheck> defaultTests = new ArrayList<>();

  public StartupChecks() {
    defaultTests.add(checkJMXPort);
    defaultTests.add(checkJDK);
  }

  public StartupChecks withDefaultTest() {
    preChecks.addAll(defaultTests);
    return this;
  }

  public StartupChecks withMoreTest(StartupCheck check) {
    preChecks.add(check);
    return this;
  }

  /**
   * execute every pretests.
   */
  public void verify() throws StartupException {
    for (StartupCheck check : preChecks) {
      check.execute();
    }
  }
}
