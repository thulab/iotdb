/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.common.constant;

/***
 * this class define several constant string variables used in tsfile schema's keys.
 *
 * @author kangrong
 *
 */
public class JsonFormatConstant {
    public static final String JSON_SCHEMA = "schema";
    public static final String DELTA_TYPE = "delta_type";
    public static final String MEASUREMENT_UID = "measurement_id";
    public static final String DATA_TYPE = "data_type";
    public static final String MEASUREMENT_ENCODING = "encoding";
    public static final String ENUM_VALUES = "enum_values";
    public static final String ENUM_VALUES_SEPARATOR = ",";
    public static final String MAX_POINT_NUMBER = "max_point_number";
    public static final String COMPRESS_TYPE = "compressor";
    public static final String FreqType = "freq_type";
    public static final String TSRECORD_SEPARATOR = ",";
    public static final String MAX_STRING_LENGTH = "max_string_length";

    public static final String ROW_GROUP_SIZE = "row_group_size";
    public static final String PAGE_SIZE = "page_size";

    public static final String defaultDeltaType = "default_delta_type";
    public static final String PROPERTIES = "properties";
}
