package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.*;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.LinkedHashMap;


public class DataSetWithTimeGenerator implements QueryDataSet {

    private TimestampGenerator timestampGenerator;
    private LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries;

    public DataSetWithTimeGenerator(TimestampGenerator timestampGenerator, LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries) {
        this.timestampGenerator = timestampGenerator;
        this.readersOfSelectedSeries = readersOfSelectedSeries;
    }

    @Override
    public boolean hasNext() throws IOException {
        return timestampGenerator.hasNext();
    }

    @Override
    public RowRecord next() throws IOException {
        long timestamp = timestampGenerator.next();
        RowRecord rowRecord = new RowRecord(timestamp);
        for (Path path : readersOfSelectedSeries.keySet()) {
            SeriesReaderByTimestamp seriesReaderByTimestamp = readersOfSelectedSeries.get(path);
            TsPrimitiveType tsPrimitiveType;
            Object value = seriesReaderByTimestamp.getValueInTimestamp(timestamp);
            TSDataType dataType = seriesReaderByTimestamp.getDataType();

            switch (dataType) {
                case INT32:
                    tsPrimitiveType = new TsPrimitiveType.TsInt((int) value);
                    break;
                case INT64:
                    tsPrimitiveType = new TsPrimitiveType.TsLong((long) value);
                    break;
                case FLOAT:
                    tsPrimitiveType = new TsPrimitiveType.TsFloat((float) value);
                    break;
                case DOUBLE:
                    tsPrimitiveType = new TsPrimitiveType.TsDouble((double) value);
                    break;
                case BOOLEAN:
                    tsPrimitiveType = new TsPrimitiveType.TsBoolean((boolean) value);
                    break;
                case TEXT:
                    tsPrimitiveType = new TsPrimitiveType.TsBinary((Binary) value);
                    break;
                default:
                    throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(dataType));

            }
            rowRecord.putField(path, tsPrimitiveType);
        }
        return rowRecord;
    }

    @Override
    public boolean hasNextV2() throws IOException {
        return timestampGenerator.hasNext();
    }

    @Override
    public RowRecordV2 nextV2() throws IOException {
        long timestamp = timestampGenerator.next();
        RowRecordV2 rowRecord = new RowRecordV2(timestamp);
        for (Path path : readersOfSelectedSeries.keySet()) {
            SeriesReaderByTimestamp seriesReaderByTimestamp = readersOfSelectedSeries.get(path);
            TSDataType dataType = seriesReaderByTimestamp.getDataType();
            Field field = new Field(dataType, path.getDeviceToString(), path.getMeasurementToString());
            Object value = seriesReaderByTimestamp.getValueInTimestampV2(timestamp);
            if (value == null) {
                field.setNull();
            }
            switch (dataType) {
                case TEXT:
                    field.setBinaryV((Binary) value);
                    break;
                case BOOLEAN:
                    field.setBoolV((boolean) value);
                    break;
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
                default:
                    throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(dataType));
            }
            rowRecord.addField(field);
        }
        return rowRecord;
    }
}
