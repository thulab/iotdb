package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.read.reader.QueryByTimestampsReader;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.LinkedHashMap;

public class QueryDataSetForQueryWithQueryFilterImpl extends QueryDataSet {

    private TimeGenerator timestampGenerator;
    private LinkedHashMap<Path, QueryByTimestampsReader> readersOfSelectedSeries;

    public QueryDataSetForQueryWithQueryFilterImpl(TimeGenerator timestampGenerator, LinkedHashMap<Path, QueryByTimestampsReader> readersOfSelectedSeries){
        super(null, null);
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
            QueryByTimestampsReader seriesReaderByTimestamp = readersOfSelectedSeries.get(path);
//            rowRecord.addField(path, seriesReaderByTimestamp.getValueInTimestamp(timestamp));
        }
        return rowRecord;
    }
}
