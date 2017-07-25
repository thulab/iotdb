package cn.edu.thu.tsfiledb.query.engine;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggreFuncFactory;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 *
 */
public class AggregateEngine {

    /**
     * filters.left is time filter, it's a SingleSeriesFilter,<br>
     * filters.right is value filter, it may be SingleSeriesFilter or CrossSeriesFilter.<br>
     * the relation between filters is disjunctive.
     *
     *
     * @param aggList
     * @param filters
     */
    public void aggregate(List<Pair<Path, String>> aggList, List<FilterStructure> filters)
            throws PathErrorException, ProcessorException {

        List<AggregateFunction> aggregateFunctionList = new ArrayList<>();
        for (Pair<Path, String> pair : aggList) {
            AggregateFunction func = AggreFuncFactory.getAggrFuncByName(pair.right, MManager.getInstance().getSeriesType(pair.left.getFullPath()));
            aggregateFunctionList.add(func);
        }


        // calculate common timestamps according to filters

        int fetchSize = 10000;
        List<CrossQueryTimeGenerator> timeGeneratorList = new ArrayList<>();
        for (FilterStructure structure : filters) {
            CrossQueryTimeGenerator timeGenerator = new CrossQueryTimeGenerator(structure.getTimeFilter(), structure.getFrequencyFilter(),
                    structure.getValueFilter(), fetchSize) {
                @Override
                public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize, SingleSeriesFilterExpression valueFilter)
                        throws ProcessorException, IOException {
                    return null;
                }
            };
            timeGeneratorList.add(timeGenerator);
        }


        // the reset of InsertDynamicOneColumnData is not expensive?


    }
}
