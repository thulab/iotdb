package cn.edu.tsinghua.iotdb.qp.physical.crud;

import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.lineFeedSignal;
import java.util.*;

import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

/**
 * This class is constructed with a single query plan. Single query means it could be processed by
 * TsFile reading API by one pass directly.<br>
 * Up to now, Single Query that {@code TsFile reading API} supports is a conjunction among time
 * filter, frequency filter and value filter. <br>
 * This class provide two public function. If the whole SingleQueryPlan has exactly one single path,
 * {@code SingleQueryPlan} return a {@code Iterator<OnePassQueryDataSet>} directly. Otherwise
 * {@code SingleQueryPlan} is regard as a portion of {@code MultiQueryPlan}. This class provide
 * a {@code Iterator<RowRecord>}in the latter case.
 *
 */
public class SingleQueryPlan extends PhysicalPlan {

    private static final Logger LOG = LoggerFactory.getLogger(SingleQueryPlan.class);
    private List<Path> paths = new ArrayList<>();
    private List<String> aggregations = new ArrayList<>();
    private FilterOperator timeFilterOperator;
    private FilterOperator freqFilterOperator;
    private FilterOperator valueFilterOperator;
    private QueryFilter[] QueryFilters;

    //TODO 合并多种 filter
    public SingleQueryPlan(List<Path> paths, FilterOperator timeFilter,
                           FilterOperator freqFilter, FilterOperator valueFilter,
                           QueryProcessExecutor executor, List<String> aggregations) throws QueryProcessorException {
        super(true, Operator.OperatorType.QUERY);
        this.paths = paths;
        this.timeFilterOperator = timeFilter;
        this.freqFilterOperator = freqFilter;
        this.valueFilterOperator = valueFilter;
        checkPaths(executor);
        QueryFilters = transformToQueryFilters(executor);
        this.aggregations = aggregations;
    }

    public void setAggregations(List<String> aggregations) {
        this.aggregations = aggregations;
    }

    public List<String> getAggregations() {
        return aggregations;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    /**
     * QueryFilters include three QueryFilter: TIME_FILTER, FREQUENCY_FILTER, VALUE_FILTER
     * These filters is for querying data in TsFile
     *
     * @return three filter expressions
     */
    public QueryFilter[] getQueryFilters() {
        return QueryFilters;
    }

    /**
     * check if all paths exist
     */
    private void checkPaths(QueryProcessExecutor executor) throws QueryProcessorException {
        for (Path path : paths) {
            if (!executor.judgePathExists(path))
                throw new QueryProcessorException("Path doesn't exist: " + path);
        }
    }

    /**
     * convert filter operators to filter expressions
     *
     * @param executor query process executor
     * @return three filter expressions
     * @throws QueryProcessorException exceptions in transforming filter operators
     */
    private QueryFilter[] transformToQueryFilters(QueryProcessExecutor executor)
            throws QueryProcessorException {
        QueryFilter timeFilter =
                timeFilterOperator == null ? null : timeFilterOperator.transformToQueryFilter(executor, QueryFilterType.TIME_FILTER);
        QueryFilter freqFilter =
                freqFilterOperator == null ? null : freqFilterOperator.transformToQueryFilter(executor, QueryFilterType.FREQUENCY_FILTER);
        QueryFilter valueFilter =
                valueFilterOperator == null ? null : valueFilterOperator.transformToQueryFilter(executor, QueryFilterType.VALUE_FILTER);

        if (valueFilter instanceof SeriesFilter) {
            if (paths.size() == 1) {
                FilterSeries<?> series = ((SeriesFilter) valueFilter).getFilterSeries();
                Path path = paths.get(0);
                if (!series.getDeltaObjectUID().equals(path.getDevice())
                        || !series.getMeasurementUID().equals(path.getMeasurement())) {
                    valueFilter = FilterFactory.and(valueFilter, valueFilter);
                }
            } else
                valueFilter = FilterFactory.and(valueFilter, valueFilter);
        }
        return new QueryFilter[]{timeFilter, freqFilter, valueFilter};
    }


    /**
     * @param executor query process executor
     * @return Iterator<RowRecord>
     */
    private Iterator<OldRowRecord> getRecordIterator(QueryProcessExecutor executor, int formNumber) throws QueryProcessorException {

        return new RowRecordIterator(formNumber,paths, executor.getFetchSize(), executor, QueryFilters[0], QueryFilters[1], QueryFilters[2]);
    }


    public static Iterator<OldRowRecord>[] getRecordIteratorArray(List<SingleQueryPlan> plans,
                                                               QueryProcessExecutor conf) throws QueryProcessorException {
        Iterator<OldRowRecord>[] ret = new RowRecordIterator[plans.size()];
        for (int i = 0; i < plans.size(); i++) {
            ret[i] = plans.get(i).getRecordIterator(conf, i);
        }
        return ret;
    }

    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer();
        String preSpace = "  ";
        sc.addTail("SingleQueryPlan:", lineFeedSignal);
        sc.addTail(preSpace, "paths:  ").addTail(paths.toString(), lineFeedSignal);
        sc.addTail(preSpace, timeFilterOperator == null ? "null" : timeFilterOperator.toString(),
                lineFeedSignal);
        sc.addTail(preSpace, freqFilterOperator == null ? "null" : freqFilterOperator.toString(),
                lineFeedSignal);
        sc.addTail(preSpace, valueFilterOperator == null ? "null" : valueFilterOperator.toString(),
                lineFeedSignal);
        return sc.toString();
    }

    @Override
    public List<Path> getPaths() {
        return paths;
    }

    private class RowRecordIterator implements Iterator<OldRowRecord> {
        private boolean noNext = false;
        private List<Path> paths;
        private final int fetchSize;
        private final QueryProcessExecutor executor;
        private OnePassQueryDataSet data = null;
        private QueryFilter timeFilter;
        private QueryFilter freqFilter;
        private QueryFilter valueFilter;
        private int formNumber;

        public RowRecordIterator(int formNumber, List<Path> paths, int fetchSize, QueryProcessExecutor executor,
                                 QueryFilter timeFilter, QueryFilter freqFilter,
                                 QueryFilter valueFilter) {
            this.formNumber = formNumber;
            this.paths = paths;
            this.fetchSize = fetchSize;
            this.executor = executor;
            this.timeFilter = timeFilter;
            this.freqFilter = freqFilter;
            this.valueFilter = valueFilter;
        }

        @Override
        public boolean hasNext() {
            if (noNext)
                return false;
            if (data == null || !data.hasNextRecord())
                try {
                    data = executor.query(formNumber, paths, timeFilter, freqFilter, valueFilter, fetchSize, data);
                } catch (ProcessorException e) {
                    throw new RuntimeException(e.getMessage());
                }
            if (data.hasNextRecord())
                return true;
            else {
                noNext = true;
                return false;
            }
        }

        @Override
        public OldRowRecord next() {
            return data.getNextRecord();
        }

    }
}
