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
package org.apache.iotdb.db.postback.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lta
 */
public class PostBackSenderDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostBackSenderDescriptor.class);
  private PostBackSenderConfig conf = new PostBackSenderConfig();

  private PostBackSenderDescriptor() {
    loadProps();
  }

  public static final PostBackSenderDescriptor getInstance() {
    return PostBackDescriptorHolder.INSTANCE;
  }

  public PostBackSenderConfig getConfig() {
    return conf;
  }

  public void setConfig(PostBackSenderConfig conf) {
    this.conf = conf;
  }

  /**
   * load an properties file and set TsfileDBConfig variables
   *
   */
  private void loadProps() {
    InputStream inputStream = null;
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar
            + PostBackSenderConfig.CONFIG_NAME;
      } else {
        LOGGER.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration",
            PostBackSenderConfig.CONFIG_NAME);
        return;
      }
    } else {
      url += (File.separatorChar + PostBackSenderConfig.CONFIG_NAME);
    }

    try {
      inputStream = new FileInputStream(new File(url));
    } catch (FileNotFoundException e) {
      LOGGER.warn("Fail to find config file {}", url);
      // update all data seriesPath
      return;
    }

    LOGGER.info("Start to read config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);

      conf.serverIp = properties.getProperty("server_ip", conf.serverIp);
      conf.serverPort = Integer
          .parseInt(properties.getProperty("server_port", conf.serverPort + ""));

      conf.clientPort = Integer
          .parseInt(properties.getProperty("client_port", conf.clientPort + ""));
      conf.uploadCycleInSeconds = Integer
          .parseInt(
              properties.getProperty("upload_cycle_in_seconds", conf.uploadCycleInSeconds + ""));
      conf.schemaPath = properties.getProperty("iotdb_schema_directory", conf.schemaPath);
      conf.isClearEnable = Boolean
          .parseBoolean(properties.getProperty("is_clear_enable", conf.isClearEnable + ""));
      conf.uuidPath = conf.dataDirectory + "postback" + File.separator + "uuid.txt";
      conf.lastFileInfo =
          conf.dataDirectory + "postback" + File.separator + "lastLocalFileList.txt";

      String[] snapshots = new String[conf.iotdbBufferwriteDirectory.length];
      for (int i = 0; i < conf.iotdbBufferwriteDirectory.length; i++) {
        conf.iotdbBufferwriteDirectory[i] = new File(conf.iotdbBufferwriteDirectory[i])
            .getAbsolutePath();
        if (!conf.iotdbBufferwriteDirectory[i].endsWith(File.separator)) {
          conf.iotdbBufferwriteDirectory[i] = conf.iotdbBufferwriteDirectory[i] + File.separator;
        }
        snapshots[i] =
            conf.iotdbBufferwriteDirectory[i] + "postback" + File.separator + "dataSnapshot"
                + File.separator;
      }
      conf.snapshotPaths = snapshots;
    } catch (IOException e) {
      LOGGER.warn("Cannot load config file because {}, use default configuration", e.getMessage());
    } catch (Exception e) {
      LOGGER.warn("Error format in config file because {}, use default configuration",
          e.getMessage());
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOGGER.error("Fail to close config file input stream because {}", e.getMessage());
        }
      }
    }
  }

  private static class PostBackDescriptorHolder {

    private static final PostBackSenderDescriptor INSTANCE = new PostBackSenderDescriptor();
  }
}