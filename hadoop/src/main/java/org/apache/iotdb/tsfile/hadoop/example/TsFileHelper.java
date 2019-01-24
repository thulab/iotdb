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
package org.apache.iotdb.tsfile.hadoop.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileWriter;
import org.apache.iotdb.tsfile.common.utils.TsRandomAccessFileWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsRandomAccessLocalFileReader;
import org.apache.iotdb.tsfile.timeseries.basis.TsFile;
import org.apache.iotdb.tsfile.write.exception.WriteProcessException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileHelper.class);

  public static void deleteTsFile(String filePath) {
    File file = new File(filePath);
    file.delete();
  }

  public static void writeTsFile(String filePath) {

    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    conf.pageSizeInByte = 100;
    conf.groupSizeInByte = 2000;
    conf.pageCheckSizeThreshold = 1;
    conf.maxStringLength = 2;

    File file = new File(filePath);

    if (file.exists()) {
      file.delete();
    }

    JSONObject jsonSchema = getJsonSchema();

    try {
      ITsRandomAccessFileWriter output = new TsRandomAccessFileWriter(new File(filePath));
      TsFile tsFile = new TsFile(output, jsonSchema);
      String line = "";
      for (int i = 1; i < 1000; i++) {
        line = "root.car.d1," + i + ",s1," + i + ",s2,1,s3,0.1,s4,0.1";
        tsFile.writeLine(line);
      }
      tsFile.writeLine("root.car.d2,5, s1, 5, s2, 50, s3, 200.5, s4, 0.5");
      tsFile.writeLine("root.car.d2,6, s1, 6, s2, 60, s3, 200.6, s4, 0.6");
      tsFile.writeLine("root.car.d2,7, s1, 7, s2, 70, s3, 200.7, s4, 0.7");
      tsFile.writeLine("root.car.d2,8, s1, 8, s2, 80, s3, 200.8, s4, 0.8");
      tsFile.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e.getMessage());
    } catch (WriteProcessException e) {
      e.printStackTrace();
      throw new RuntimeException(e.getMessage());
    }
  }

  private static JSONObject getJsonSchema() {
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    JSONObject s1 = new JSONObject();
    s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
    s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
    s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

    JSONObject s2 = new JSONObject();
    s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
    s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
    s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

    JSONObject s3 = new JSONObject();
    s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
    s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
    s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

    JSONObject s4 = new JSONObject();
    s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
    s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
    s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

    JSONArray measureGroup = new JSONArray();
    measureGroup.put(s1);
    measureGroup.put(s2);
    measureGroup.put(s3);
    measureGroup.put(s4);

    JSONObject jsonSchema = new JSONObject();
    jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
    jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);
    return jsonSchema;
  }

  public static void main(String[] args) throws FileNotFoundException, IOException {
    String filePath = "example_mr.tsfile";
    File file = new File(filePath);
    file.delete();
    writeTsFile(filePath);
    TsFile tsFile = new TsFile(new TsRandomAccessLocalFileReader(filePath));
    LOGGER.info("Get columns information: {}", tsFile.getAllColumns());
    LOGGER.info("Get all deltaObjectId: {}", tsFile.getAllDeltaObject());
    tsFile.close();
  }
}
