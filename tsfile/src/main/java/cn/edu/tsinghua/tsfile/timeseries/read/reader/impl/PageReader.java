package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteForEncodingUtils;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType.*;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author Jinrui Zhang
 */

public class PageReader implements Reader {

    private TSDataType dataType;

    // decoder for value column
    private Decoder valueDecoder;

    // decoder for time column
    private Decoder timeDecoder;

    // time column in memory
    private ByteBuffer timeBuffer;

    // value column in memory
    private ByteBuffer valueBuffer;


    public PageReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) throws IOException {
        this.dataType = dataType;
        this.valueDecoder = valueDecoder;
        this.timeDecoder = timeDecoder;
        splitDataToTimeStampAndValue(pageData);
    }

    /**
     * split pageContent into two stream: time and value
     * @param pageData uncompressed bytes size of time column, time column, value column
     * @throws IOException exception in reading data from pageContent
     */
    private void splitDataToTimeStampAndValue(ByteBuffer pageData) throws IOException {
        int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

        timeBuffer= pageData.slice();
        timeBuffer.limit(timeBufferLength);

        valueBuffer= pageData.slice();
        valueBuffer.position(timeBufferLength);
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeDecoder.hasNext(timeBuffer);
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            TsPrimitiveType value = readOneValue();
            return new TimeValuePair(timestamp, value);
        } else {
            throw new IOException("No more TimeValuePair in current page");
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() {
        timeBuffer = null;
        valueBuffer = null;
    }

    // read one value according to data type
    private TsPrimitiveType readOneValue() {
        switch (dataType) {
            case BOOLEAN:
                return new TsBoolean(valueDecoder.readBoolean(valueBuffer));
            case INT32:
                return new TsInt(valueDecoder.readInt(valueBuffer));
            case INT64:
                return new TsLong(valueDecoder.readLong(valueBuffer));
            case FLOAT:
                return new TsFloat(valueDecoder.readFloat(valueBuffer));
            case DOUBLE:
                return new TsDouble(valueDecoder.readDouble(valueBuffer));
            case TEXT:
                return new TsBinary(valueDecoder.readBinary(valueBuffer));
            default:
                break;
        }
        throw new UnSupportedDataTypeException("Unsupported data type :" + dataType);
    }


}
