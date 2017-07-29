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
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class QueryIterator {
    private QueryDataSet queryDataSet;
    private OverflowQueryEngine overflowQueryEngine;
    private FilterExpression fe = null;
    private int readToken;
    private List<Path> pathList;

    public QueryIterator(QueryDataSet dataSet, OverflowQueryEngine overflowQueryEngine, Path path, List<Pair<Long, Long>> timeIntervals,
                         int readToken) throws ProcessorException, PathErrorException, IOException {

        pathList = new ArrayList<>();
        pathList.add(path);

        for (int i = 0; i < timeIntervals.size(); i++) {
            Pair<Long, Long> pair = timeIntervals.get(i);
            FilterSeries<Long> timeSeries = FilterFactory.timeFilterSeries();
            GtEq gtEq = FilterFactory.gtEq(timeSeries, pair.left, true);
            LtEq ltEq = FilterFactory.ltEq(timeSeries, pair.right, true);
            if (i == 0) {
                fe = FilterFactory.and(gtEq, ltEq);
            } else {
                And tmpAnd = (And) FilterFactory.and(gtEq, ltEq);
                fe = FilterFactory.or(fe, tmpAnd);
            }
        }

        this.overflowQueryEngine = overflowQueryEngine;
        this.readToken = readToken;
        this.queryDataSet = this.overflowQueryEngine.readOneColumnUseFilter(pathList, (SingleSeriesFilterExpression) fe, null, null,
                null, 10000, readToken);

    }

    public boolean hasNext() throws IOException, ProcessorException {
        if (queryDataSet.next()) {
            return true;
        } else {
            queryDataSet = overflowQueryEngine.readOneColumnUseFilter(pathList, (SingleSeriesFilterExpression) fe, null, null,
                    queryDataSet, 10000, readToken);
        }

        if (queryDataSet.next()) {
            return true;
        }
        return false;
    }

    public RowRecord getRowRecord() {
        return queryDataSet.getCurrentRecord();
    }
}
