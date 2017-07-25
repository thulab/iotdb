package cn.edu.thu.tsfiledb.query.engine;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggreFuncFactory;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;

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
    public void aggregate(List<Pair<Path, String>> aggList, List<Pair<FilterExpression, FilterExpression>> filters)
            throws PathErrorException, ProcessorException {
        List<AggregateFunction> aggregateFunctions = new ArrayList<>();
        for (Pair<Path, String> pair : aggList) {
            AggregateFunction func = AggreFuncFactory.getAggrFuncByName(pair.right, MManager.getInstance().getSeriesType(pair.left.getFullPath()));
            aggregateFunctions.add(func);
        }


    }
}
