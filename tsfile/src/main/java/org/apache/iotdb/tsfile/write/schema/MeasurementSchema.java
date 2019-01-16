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
package org.apache.iotdb.tsfile.write.schema;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.utils.ReadWriteIoUtils;
import org.apache.iotdb.tsfile.compress.Compressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class describes a measurement's information registered in {@linkplain FileSchema FilSchema}, including
 * measurement id, data type, encoding and compressor type. For each TSEncoding, MeasurementSchema maintains respective
 * TSEncodingBuilder; For TSDataType, only ENUM has TSDataTypeConverter up to now.
 *
 * @author kangrong
 * @since version 0.1.0
 */
public class MeasurementSchema implements Comparable<MeasurementSchema> {
    private static final Logger LOG = LoggerFactory.getLogger(MeasurementSchema.class);
    private TSDataType type;
    private TSEncoding encoding;
    private String measurementId;
    private TSEncodingBuilder encodingConverter;
    private Compressor compressor;
    private TSFileConfig conf;
    private Map<String, String> props = new HashMap<>();

    public MeasurementSchema() {
    }

    /**
     * set properties as an empty Map
     */
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) {
        this(measurementId, type, encoding,
                CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().compressor), Collections.emptyMap());
    }

    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding,
            CompressionType compressionType) {
        this(measurementId, type, encoding, compressionType, Collections.emptyMap());
    }

    /**
     *
     * @param measurementId
     * @param type
     * @param encoding
     * @param props
     *            information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN, Encoder.MAX_STRING_LENGTH
     */
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding,
            CompressionType compressionType, Map<String, String> props) {
        this.type = type;
        this.measurementId = measurementId;
        this.encoding = encoding;
        this.props = props == null ? Collections.emptyMap() : props;
        // get config from TSFileDescriptor
        this.conf = TSFileDescriptor.getInstance().getConfig();
        // initialize TSEncoding. e.g. set max error for PLA and SDT
        encodingConverter = TSEncodingBuilder.getConverter(encoding);
        encodingConverter.initFromProps(props);
        this.compressor = Compressor.getCompressor(compressionType);
    }

    public String getMeasurementId() {
        return measurementId;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setMeasurementId(String measurementId) {
        this.measurementId = measurementId;
    }

    public TSEncoding getEncodingType() {
        return encoding;
    }

    public TSDataType getType() {
        return type;
    }

    /**
     * return the max possible length of given type.
     *
     * @return length in unit of byte
     */
    public int getTypeLength() {
        switch (type) {
        case BOOLEAN:
            return 1;
        case INT32:
            return 4;
        case INT64:
            return 8;
        case FLOAT:
            return 4;
        case DOUBLE:
            return 8;
        case TEXT:
            // 4 is the length of string in type of Integer.
            // Note that one char corresponding to 3 byte is valid only in 16-bit BMP
            return conf.maxStringLength * TSFileConfig.BYTE_SIZE_PER_CHAR + 4;
        default:
            throw new UnSupportedDataTypeException(type.toString());
        }
    }

    public Encoder getTimeEncoder() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        TSEncoding timeSeriesEncoder = TSEncoding.valueOf(conf.timeSeriesEncoder);
        TSDataType timeType = TSDataType.valueOf(conf.timeSeriesDataType);
        return TSEncodingBuilder.getConverter(timeSeriesEncoder).getEncoder(timeType);
    }

    /**
     * get Encoder of value from encodingConverter by measurementID and data type
     * 
     * @return Encoder for value
     */
    public Encoder getValueEncoder() {
        return encodingConverter.getEncoder(type);
    }

    public Compressor getCompressor() {
        return compressor;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIoUtils.write(measurementId, outputStream);

        byteLen += ReadWriteIoUtils.write(type, outputStream);

        byteLen += ReadWriteIoUtils.write(encoding, outputStream);

        byteLen += ReadWriteIoUtils.write(compressor.getType(), outputStream);

        if (props == null) {
            byteLen += ReadWriteIoUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteIoUtils.write(props.size(), outputStream);
            for (Map.Entry<String, String> entry : props.entrySet()) {
                byteLen += ReadWriteIoUtils.write(entry.getKey(), outputStream);
                byteLen += ReadWriteIoUtils.write(entry.getValue(), outputStream);
            }
        }

        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIoUtils.write(measurementId, buffer);

        byteLen += ReadWriteIoUtils.write(type, buffer);

        byteLen += ReadWriteIoUtils.write(encoding, buffer);

        byteLen += ReadWriteIoUtils.write(compressor.getType(), buffer);

        if (props == null) {
            byteLen += ReadWriteIoUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteIoUtils.write(props.size(), buffer);
            for (Map.Entry<String, String> entry : props.entrySet()) {
                byteLen += ReadWriteIoUtils.write(entry.getKey(), buffer);
                byteLen += ReadWriteIoUtils.write(entry.getValue(), buffer);
            }
        }

        return byteLen;
    }

    public static MeasurementSchema deserializeFrom(InputStream inputStream) throws IOException {
        MeasurementSchema measurementSchema = new MeasurementSchema();

        measurementSchema.measurementId = ReadWriteIoUtils.readString(inputStream);

        measurementSchema.type = ReadWriteIoUtils.readDataType(inputStream);

        measurementSchema.encoding = ReadWriteIoUtils.readEncoding(inputStream);

        CompressionType compressionType = ReadWriteIoUtils.readCompressionType(inputStream);
        measurementSchema.compressor = Compressor.getCompressor(compressionType);

        int size = ReadWriteIoUtils.readInt(inputStream);
        if (size > 0) {
            measurementSchema.props = new HashMap<>();
            String key;
            String value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIoUtils.readString(inputStream);
                value = ReadWriteIoUtils.readString(inputStream);
                measurementSchema.props.put(key, value);
            }
        }

        return measurementSchema;
    }

    public static MeasurementSchema deserializeFrom(ByteBuffer buffer) throws IOException {
        MeasurementSchema measurementSchema = new MeasurementSchema();

        measurementSchema.measurementId = ReadWriteIoUtils.readString(buffer);

        measurementSchema.type = ReadWriteIoUtils.readDataType(buffer);

        measurementSchema.encoding = ReadWriteIoUtils.readEncoding(buffer);

        CompressionType compressionType = ReadWriteIoUtils.readCompressionType(buffer);
        measurementSchema.compressor = Compressor.getCompressor(compressionType);

        int size = ReadWriteIoUtils.readInt(buffer);
        if (size > 0) {
            measurementSchema.props = new HashMap<>();
            String key;
            String value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIoUtils.readString(buffer);
                value = ReadWriteIoUtils.readString(buffer);
                measurementSchema.props.put(key, value);
            }
        }

        return measurementSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MeasurementSchema that = (MeasurementSchema) o;
        return type == that.type && encoding == that.encoding && Objects.equals(measurementId, that.measurementId)
                && Objects.equals(encodingConverter, that.encodingConverter)
                && Objects.equals(compressor, that.compressor) && Objects.equals(conf, that.conf)
                && Objects.equals(props, that.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, encoding, measurementId, encodingConverter, compressor, conf, props);
    }

    /**
     * compare by measurementID
     */
    @Override
    public int compareTo(MeasurementSchema o) {
        if (equals(o))
            return 0;
        else
            return this.measurementId.compareTo(o.measurementId);
    }

    @Override
    public String toString() {
        StringContainer sc = new StringContainer("");
        sc.addTail("[", measurementId, ",", type.toString(), ",", encoding.toString(), ",", props.toString(), ",",
                compressor.getType().toString());
        sc.addTail("]");
        return sc.toString();
    }
}
