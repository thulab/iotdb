package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.LinkedHashMap;

public class QueryDataSetForQueryWithQueryFilterImpl implements QueryDataSet {

    private TimestampGenerator timestampGenerator;
    private LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries;

    public QueryDataSetForQueryWithQueryFilterImpl(TimestampGenerator timestampGenerator, LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries){
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
            SeriesReaderByTimeStamp seriesReaderByTimestamp = readersOfSelectedSeries.get(path);
            rowRecord.putField(path, seriesReaderByTimestamp.getValueInTimestamp(timestamp));
        }
        return rowRecord;
    }
}
