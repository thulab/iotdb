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
package org.apache.iotdb.tsfile.write.schema.converter;

import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.SchemaBuilder;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author qiaojialin
 */
public class SchemaBuilderTest {
    @Test
    public void testJsonConverter() {

        SchemaBuilder builder = new SchemaBuilder();
        Map<String, String> props = new HashMap<>();
        props.put("enum_values", "[\"MAN\",\"WOMAN\"]");
        props.clear();
        props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
        builder.addSeries("s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props);
        builder.addSeries("s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null);
        FileSchema fileSchema = builder.build();

        Collection<MeasurementSchema> measurements = fileSchema.getAllMeasurementSchema().values();
        String[] measureDesStrings = { "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]",
                "[s5,INT32,TS_2DIFF,{},UNCOMPRESSED]" };
        int i = 0;
        for (MeasurementSchema desc : measurements) {
            assertEquals(measureDesStrings[i++], desc.toString());
        }
    }
}
