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
package cn.edu.tsinghua.iotdb.api;

/**
 * Created by liukun on 19/3/7.
 */
public class IoTDBOptions {

  private String walPath = "/data2/iotdb";

  /**
   * 提供接口set 和 get对应的TsFileConfig和IoTDBConfig信息
   *
   * 调整配置信息需要open系统之前就进行配置
   */

  public IoTDBOptions() {

  }

  public String getWalPath() {
    return walPath;
  }

  public void setWalPath(String walPath) {
    this.walPath = walPath;
  }


  private long periodTimeForFlush = 3600;

  public long getPeriodTimeForFlush() {
    return periodTimeForFlush;
  }

  public void setPeriodTimeForFlush(long periodTimeForFlush) {
    this.periodTimeForFlush = periodTimeForFlush;
  }

  private long periodTimeForMerge = 7200;

  public long getPeriodTimeForMerge() {
    return periodTimeForMerge;
  }

  public void setPeriodTimeForMerge(long periodTimeForMerge) {
    this.periodTimeForMerge = periodTimeForMerge;
  }

  private long bufferwriteMetaSizeThreshold = 200 * 1024 * 1024L;
  ;

  public long getBufferwriteMetaSizeThreshold() {
    return bufferwriteMetaSizeThreshold;
  }

  public void setBufferwriteMetaSizeThreshold(long bufferwriteMetaSizeThreshold) {
    this.bufferwriteMetaSizeThreshold = bufferwriteMetaSizeThreshold;
  }

  private long bufferwriteFileSizeThreshold = 2 * 1024 * 1024 * 1024L;

  public long getBufferwriteFileSizeThreshold() {
    return bufferwriteFileSizeThreshold;
  }

  public void setBufferwriteFileSizeThreshold(long bufferwriteFileSizeThreshold) {
    this.bufferwriteFileSizeThreshold = bufferwriteFileSizeThreshold;
  }

  private long overflowMetaSizeThreshold = 20 * 1024 * 1024L;

  public long getOverflowMetaSizeThreshold() {
    return overflowMetaSizeThreshold;
  }

  public void setOverflowMetaSizeThreshold(long overflowMetaSizeThreshold) {
    this.overflowMetaSizeThreshold = overflowMetaSizeThreshold;
  }

  private long overflowFileSizeThreshold = 200 * 1024 * 1024L;

  public long getOverflowFileSizeThreshold() {
    return overflowFileSizeThreshold;
  }

  public void setOverflowFileSizeThreshold(long overflowFileSizeThreshold) {
    this.overflowFileSizeThreshold = overflowFileSizeThreshold;
  }
}
