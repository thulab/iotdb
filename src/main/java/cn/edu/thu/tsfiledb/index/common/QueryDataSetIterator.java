package cn.edu.thu.tsfiledb.index.common;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This is an iterator wrap class used for multi-batch fetching query data set in query process.
 *
 * @author CGF, Jiaye Wu
 */
public class QueryDataSetIterator {

    private OverflowQueryEngine overflowQueryEngine;

    private QueryDataSet queryDataSet;

    private List<Path> pathList;

    private FilterExpression filterExpression;

    private int readToken;

    public QueryDataSetIterator(OverflowQueryEngine overflowQueryEngine, Path path, List<Pair<Long, Long>> timeIntervals,
                                int readToken) throws ProcessorException, PathErrorException, IOException {
        pathList = Collections.singletonList(path);

        for (int i = 0; i < timeIntervals.size(); i++) {
            Pair<Long, Long> pair = timeIntervals.get(i);
            FilterSeries<Long> timeSeries = FilterFactory.timeFilterSeries();
            GtEq gtEq = FilterFactory.gtEq(timeSeries, pair.left, true);
            LtEq ltEq = FilterFactory.ltEq(timeSeries, pair.right, true);
            if (i == 0) {
                filterExpression = FilterFactory.and(gtEq, ltEq);
            } else {
                And tmpAnd = (And) FilterFactory.and(gtEq, ltEq);
                filterExpression = FilterFactory.or(filterExpression, tmpAnd);
            }
        }

        this.overflowQueryEngine = overflowQueryEngine;
        this.readToken = readToken;
        this.queryDataSet = this.overflowQueryEngine.readOneColumnUseFilter(pathList, (SingleSeriesFilterExpression) filterExpression, null, null,
                null, TsfileDBDescriptor.getInstance().getConfig().fetchSize, readToken);
    }

    public boolean hasNext() throws IOException, ProcessorException {
        if (queryDataSet.next()) {
            return true;
        } else {
            queryDataSet = overflowQueryEngine.readOneColumnUseFilter(pathList, (SingleSeriesFilterExpression) filterExpression, null, null,
                    queryDataSet, TsfileDBDescriptor.getInstance().getConfig().fetchSize, readToken);
            return queryDataSet.next();
        }
    }

    public RowRecord getRowRecord() {
        return queryDataSet.getCurrentRecord();
    }
}
