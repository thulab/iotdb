/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iotdb.api.example;

import cn.edu.tsinghua.iotdb.api.ITSEngine;
import cn.edu.tsinghua.iotdb.api.IoTDBEngineException;
import cn.edu.tsinghua.iotdb.api.IoTDBOptions;
import cn.edu.tsinghua.iotdb.api.impl.IoTDBEngine;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;

/**
 * Created by liukun on 19/3/8.
 */
public class MultiThreadWrite {

  private static class DataRow {

    private String deviceId;
    private long time;
    private List<String> sensors;
    private List<String> values;
  }

  private static class Workload {

    private final int device_num;
    private List<String> devices;
    private List<String> sensors;
    private List<String> values;
    private final int row_num;
    private AtomicInteger index;

    private Workload(int device_num, int sensor_num, int row_num) {
      this.device_num = device_num;
      this.row_num = row_num;
      this.index = new AtomicInteger();
      this.devices = new ArrayList<>();
      this.sensors = new ArrayList<>();
      this.values = new ArrayList<>();
      for (int i = 0; i < device_num; i++) {
        devices.add("root.beijing.d" + i);
      }
      for (int i = 0; i < sensor_num; i++) {
        sensors.add("s" + i);
      }
      for (int i = 0; i < sensor_num; i++) {
        values.add("0");
      }
    }

    private boolean hasNext() {
      return index.get() < row_num;

    }

    private DataRow next() {
      int cur = index.getAndIncrement();
      DataRow dataRow = new DataRow();
      dataRow.deviceId = devices.get(cur % device_num);
      dataRow.sensors = sensors;
      dataRow.values = values;
      dataRow.time = cur;
      return dataRow;
    }
  }

  private static final int client_num = 5;
  private static final int device_num = 10;
  private static final int sensor_num = 10;
  private static final int row_num = 10000;

  public static class Worker implements Runnable {

    private ITSEngine db;
    private Workload workload;
    private CountDownLatch latch;

    public Worker(ITSEngine db, Workload workload, CountDownLatch latch) {
      this.db = db;
      this.workload = workload;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        while (workload.hasNext()) {
          DataRow dataRow = workload.next();
          try {
            db.write(dataRow.deviceId, dataRow.time, dataRow.sensors, dataRow.values);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      } finally {
        latch.countDown();
      }
    }
  }

  private static void createMetadata(ITSEngine engine) throws IOException {
    String storageGroup = "root.beijing";
    engine.setStorageGroup(storageGroup);
    TSDataType dataType = TSDataType.INT32;
    TSEncoding encoding = TSEncoding.PLAIN;
    for (int i = 0; i < device_num; i++) {
      for (int j = 0; j < sensor_num; j++) {
        String timeseries = storageGroup + "." + "d" + i + ".s" + j;
        engine.addTimeSeries(timeseries, dataType.toString(), encoding.toString(), new String[0]);
      }
    }
  }

  /**
   * 多线程写入与查询IoTDB engine
   */
  public static void main(String[] args) throws IoTDBEngineException, IOException {
    File file = new File("mul-iotdb");
    FileUtils.deleteDirectory(file);
    IoTDBOptions options = new IoTDBOptions();
    ITSEngine engine = new IoTDBEngine(file, options);
    engine.openOrCreate();
    // create metadata
    createMetadata(engine);
    Workload workload = new Workload(device_num, sensor_num, row_num);
    // multi-threads write data
    CountDownLatch latch = new CountDownLatch(client_num);
    for (int i = 0; i < client_num; i++) {
      Thread thread = new Thread(new Worker(engine, workload, latch));
      thread.start();
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // query data
    Iterator<QueryDataSet> dataSet = engine.query("root.beijing.d0.s0", 0, 5000);
    int count = 0;
    while (dataSet.hasNext()) {
      QueryDataSet queryDataSet = dataSet.next();
      while (queryDataSet.next()) {
        count++;
        System.out.println(queryDataSet.getCurrentRecord());
      }
    }
    System.out.println(count);
    engine.close();
    // delete all data
    FileUtils.deleteDirectory(file);
  }
}
