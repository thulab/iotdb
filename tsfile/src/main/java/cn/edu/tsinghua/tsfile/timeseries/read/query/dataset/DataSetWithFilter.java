package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.LinkedHashMap;


public class DataSetWithFilter implements QueryDataSet {

    private TimestampGenerator timestampGenerator;
    private LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries;

    public DataSetWithFilter(TimestampGenerator timestampGenerator, LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries) {
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
            rowRecord.putField(path, seriesReaderByTimestamp.getValueInTimestamp(timestamp));
        }
        return rowRecord;
    }
}
