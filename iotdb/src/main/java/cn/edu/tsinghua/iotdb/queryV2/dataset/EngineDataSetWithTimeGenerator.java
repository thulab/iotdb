package cn.edu.tsinghua.iotdb.queryV2.dataset;

import cn.edu.tsinghua.iotdb.queryV2.reader.merge.EngineSeriesReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.queryV2.timegenerator.EngineTimeGenerator;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.List;


public class EngineDataSetWithTimeGenerator extends QueryDataSet {

    private EngineTimeGenerator timeGenerator;
    private List<EngineSeriesReaderByTimeStamp> readers;

    public EngineDataSetWithTimeGenerator(EngineTimeGenerator timeGenerator, List<EngineSeriesReaderByTimeStamp> readers) {
        super(null, null);
        this.timeGenerator = timeGenerator;
        this.readers = readers;
    }


    @Override
    public boolean hasNext() throws IOException {
        return timeGenerator.hasNext();
    }


    @Override
    public RowRecord next() throws IOException {
        long timestamp = timeGenerator.next();
        RowRecord rowRecord = new RowRecord(timestamp);
//
//        for (int i = 0; i < paths.size(); i++) {
//
//            // get value from readers in time generator
//            if (cached.get(i)) {
//                Object value = timeGenerator.getValue(paths.get(i), timestamp);
//                rowRecord.addField(getField(value, dataTypes.get(i)));
//                continue;
//            }
//
//            // get value from series reader without filter
//            SeriesReaderByTimestamp seriesReaderByTimestamp = readers.get(i);
//            Object value = seriesReaderByTimestamp.getValueInTimestamp(timestamp);
//            rowRecord.addField(getField(value, dataTypes.get(i)));
//        }

        return rowRecord;
    }

//    private Field getField(Object value, TSDataType dataType) {
//        Field field = new Field(dataType);
//
//        if (value == null) {
//            field.setNull();
//            return field;
//        }
//        switch (dataType) {
//            case DOUBLE:
//                field.setDoubleV((double) value);
//                break;
//            case FLOAT:
//                field.setFloatV((float) value);
//                break;
//            case INT64:
//                field.setLongV((long) value);
//                break;
//            case INT32:
//                field.setIntV((int) value);
//                break;
//            case BOOLEAN:
//                field.setBoolV((boolean) value);
//                break;
//            case TEXT:
//                field.setBinaryV((Binary) value);
//                break;
//            default:
//                throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(dataType));
//        }
//        return field;
//    }
}
