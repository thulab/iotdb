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
package org.apache.iotdb.tsfile.write.schema;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.exception.write.InvalidJsonSchemaException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> JsonConverter is used to convert JsonObject to TSFile Schema which is a java class defined
 * in
 * tsfile project. the main function of this converter is to receive a json object of schema and
 * register all measurements. </p>
 *
 * <p> The format of JSON schema is as follow:
 *
 * <pre>
 *  {
 *     "schema": [
 *        {
 *          "measurement_id": "s1",
 *          "data_type": "INT32",
 *          "encoding": "RLE",
 *          "compressor": "SNAPPY"
 *         },
 *         {
 *             "measurement_id": "s3",
 *             "data_type": "ENUMS",
 *             "encoding": "BITMAP",
 *             "compressor": "SNAPPY",
 *             "enum_values":["MAN","WOMAN"],
 *             "max_error":12,
 *             "max_point_number":3
 *         },
 *         ...
 *     ]
 * }
 *
 * </pre>
 * </p>
 *
 * @author kangrong
 * @see TSEncodingBuilder TSEncodingBuilder
 */
public class JsonConverter {

  private static final Logger LOG = LoggerFactory.getLogger(JsonConverter.class);
  private JsonConverter(){}
  /**
   * input a FileSchema and a jsonObject to be converted.
   *
   * @param jsonSchema the whole schema in type of JSONObject
   * @return converted measurement descriptors
   * @throws InvalidJsonSchemaException throw exception when json schema is not valid
   */

  public static Map<String, MeasurementSchema> converterJsonToMeasurementSchemas(
      JSONObject jsonSchema)
      throws InvalidJsonSchemaException {
    Map<String, MeasurementSchema> result = new HashMap<>();
    if (!jsonSchema.has(JsonFormatConstant.JSON_SCHEMA)) {
      throw new InvalidJsonSchemaException("missing fields:" + JsonFormatConstant.JSON_SCHEMA);
    }

    /**
     * get schema of all measurements in JSONArray from JSONObject.
     * <p></p>
     * "schema": [ { "measurement_id": "s1", "data_type": "INT32", "encoding": "RLE" },
     * { "measurement_id": "s2", "data_type": "INT64", "encoding": "TS_2DIFF" }... ]
     */
    JSONArray schemaArray = jsonSchema.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
    for (int i = 0; i < schemaArray.length(); i++) {
      MeasurementSchema mdescriptor = convertJsonToMeasurementSchema(schemaArray.getJSONObject(i));
      result.put(mdescriptor.getMeasurementId(), mdescriptor);
    }
    return result;
  }

  /**
   * convert the input JSONObject to MeasurementSchema.
   *
   * @param measurementObj properties of one measurement
   *
   * an example: { "measurement_id": "s3", "data_type": "ENUMS", "encoding": "BITMAP", // some
   * measurement may have some properties "compressor": "SNAPPY", "enum_values":["MAN","WOMAN"],
   * "max_error":12, "max_point_number":3 }
   * @return converted MeasurementSchema
   */
  public static MeasurementSchema convertJsonToMeasurementSchema(JSONObject measurementObj) {
    if (!measurementObj.has(JsonFormatConstant.MEASUREMENT_UID) && !measurementObj
        .has(JsonFormatConstant.DATA_TYPE)
        && !measurementObj.has(JsonFormatConstant.MEASUREMENT_ENCODING)
        && !measurementObj.has(JsonFormatConstant.COMPRESS_TYPE)) {
      LOG.warn(
          "The format of given json is error. Give up to register this measurement. Given json:{}",
          measurementObj);
      return null;
    }
    // get measurementID
    String measurementId = measurementObj.getString(JsonFormatConstant.MEASUREMENT_UID);
    // get data type information
    TSDataType type = TSDataType.valueOf(measurementObj.getString(JsonFormatConstant.DATA_TYPE));
    // get encoding information
    TSEncoding encoding = TSEncoding
        .valueOf(measurementObj.getString(JsonFormatConstant.MEASUREMENT_ENCODING));
    CompressionType compressionType = measurementObj.has(JsonFormatConstant.COMPRESS_TYPE)
        ? CompressionType.valueOf(measurementObj.getString(JsonFormatConstant.COMPRESS_TYPE))
        : CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().compressor);
    // all information of one series
    Map<String, String> props = new HashMap<>();
    for (Object key : measurementObj.keySet()) {
      if (key.equals(JsonFormatConstant.MEASUREMENT_ENCODING) || key
          .equals(JsonFormatConstant.COMPRESS_TYPE)
          || key.equals(JsonFormatConstant.MEASUREMENT_UID) || key
          .equals(JsonFormatConstant.DATA_TYPE)) {
        continue;
      }
      String value = measurementObj.get(key.toString()).toString();
      props.put(key.toString(), value);
    }
    return new MeasurementSchema(measurementId, type, encoding, compressionType, props);
  }

  /**
   * function for converting chunk group size from jsonSchema.
   */
  public static long convertJsonToChunkGroupSize(JSONObject jsonSchema) {
    if (jsonSchema.has(JsonFormatConstant.ROW_GROUP_SIZE)) {
      return jsonSchema.getLong(JsonFormatConstant.ROW_GROUP_SIZE);
    }
    return 128L * 1024 * 1024;
  }

  /**
   * given a FileSchema and convert it into a JSONObject.
   *
   * @param fileSchema the given schema in type of {@linkplain FileSchema FileSchema}
   * @return converted File Schema in type of JSONObject
   */
  public static JSONObject converterFileSchemaToJson(FileSchema fileSchema) {
    /** JSONObject form of FileSchema. **/
    JSONObject ret = new JSONObject();
    /** JSONObject form of all MeasurementSchemas in fileSchema. **/
    JSONArray jsonSchema = new JSONArray();

    for (MeasurementSchema measurementSchema : fileSchema.getAllMeasurementSchema().values()) {
      jsonSchema.put(convertMeasurementSchemaToJson(measurementSchema));
    }

    ret.put(JsonFormatConstant.JSON_SCHEMA, jsonSchema);
    return ret;
  }

  /**
   * given a MeasurementSchema and convert it to a JSONObject.
   *
   * @param measurementSchema the given descriptor in type of {@linkplain MeasurementSchema
   * MeasurementSchema}
   * @return converted MeasurementSchema in form of JSONObject
   *
   * an example: { "measurement_id": "s3", "data_type": "ENUMS", "encoding": "BITMAP", // some
   * measurement may have some properties "compressor": "SNAPPY", "enum_values":["MAN","WOMAN"],
   * "max_error":12, "max_point_number":3 }
   */
  private static JSONObject convertMeasurementSchemaToJson(MeasurementSchema measurementSchema) {
    JSONObject measurementObj = new JSONObject();
    // put measurementID, data type, encoding info and properties into result JSONObject
    measurementObj.put(JsonFormatConstant.MEASUREMENT_UID, measurementSchema.getMeasurementId());
    measurementObj.put(JsonFormatConstant.DATA_TYPE, measurementSchema.getType().toString());
    measurementObj.put(JsonFormatConstant.MEASUREMENT_ENCODING,
        measurementSchema.getEncodingType().toString());
    measurementSchema.getProps().forEach(measurementObj::put);
    return measurementObj;
  }

}
