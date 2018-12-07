package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.LinkedHashMap;

public class QueryDataSetForQueryWithQueryFilterImpl extends QueryDataSet {

    private TimestampGenerator timestampGenerator;
    private LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries;

    public QueryDataSetForQueryWithQueryFilterImpl(TimestampGenerator timestampGenerator, LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries){
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
            TSDataType dataType = seriesReaderByTimestamp.getDataType();
            Field f = new Field(dataType);
            Object value = seriesReaderByTimestamp.getValueInTimestamp(timestamp);
            switch (dataType) {
                case TEXT:
                    f.setBinaryV((Binary) value);
                    break;
                case FLOAT:
                    f.setFloatV((float) value);
                    break;
                case INT32:
                    f.setIntV((int) value);
                    break;
                case INT64:
                    f.setLongV((long) value);
                    break;
                case DOUBLE:
                    f.setDoubleV((double) value);
                    break;
                case BOOLEAN:
                    f.setBoolV((boolean) value);
                    break;
                default:
                    throw new IOException("Unsupported datatype " + dataType);
            }
            rowRecord.addField(f);
        }
        return rowRecord;
    }
}
