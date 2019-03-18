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
package cn.edu.tsinghua.iotdb.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by liukun on 19/3/7.
 */
public interface ITSEngine {

  /**
   * Open or Create the Engine if not exist.
   * @throws IoTDBEngineException
   */
  void openOrCreate() throws IoTDBEngineException;

  /**
   * close the iotdb engine, which can only be call once.
   */
  void close() throws IOException;

  void write(String deviceId, long insertTime, List<String> measurementList,
      List<String> insertValues) throws IOException;

  Iterator<cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet> query(String timeseries, long startTime, long endTime) throws IOException;

  IoTDBOptions getOptions();

  void setStorageGroup(String storageGroup) throws IOException;

  void addTimeSeries(String path, String dataType, String encoding, String[] args)
      throws IOException;
}
