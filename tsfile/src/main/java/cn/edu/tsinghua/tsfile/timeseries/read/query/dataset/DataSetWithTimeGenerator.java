package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.*;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.List;


public class DataSetWithTimeGenerator extends QueryDataSet {

    private TimestampGenerator timestampGenerator;
    private List<SeriesReaderByTimestamp> readers;

    public DataSetWithTimeGenerator(List<Path> paths, List<TSDataType> dataTypes, TimestampGenerator timestampGenerator, List<SeriesReaderByTimestamp> readers) {
        super(paths, dataTypes);
        this.timestampGenerator = timestampGenerator;
        this.readers = readers;
    }

    @Override
    public boolean hasNext() throws IOException {
        return timestampGenerator.hasNext();
    }

    @Override
    public RowRecord next() throws IOException {
        long timestamp = timestampGenerator.next();
        RowRecord rowRecord = new RowRecord(timestamp);

        for(int i = 0; i < paths.size(); i++) {
            SeriesReaderByTimestamp seriesReaderByTimestamp = readers.get(i);
            TSDataType dataType = seriesReaderByTimestamp.getDataType();
            Field field = new Field(dataType);
            Object value = seriesReaderByTimestamp.getValueInTimestamp(timestamp);
            if (value == null) {
                field.setNull();
                continue;
            }
            switch (dataType) {
                case DOUBLE:
                    field.setDoubleV((double) value);
                    break;
                case FLOAT:
                    field.setFloatV((float) value);
                    break;
                case INT64:
                    field.setLongV((long) value);
                    break;
                case INT32:
                    field.setIntV((int) value);
                    break;
                case BOOLEAN:
                    field.setBoolV((boolean) value);
                    break;
                case TEXT:
                    field.setBinaryV((Binary) value);
                    break;
                default:
                    throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(dataType));
            }
            rowRecord.addField(field);
        }

        return rowRecord;
    }
}
