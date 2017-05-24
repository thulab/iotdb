package cn.edu.thu.tsfile.spark;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.spark.common.FilterOperator;
import cn.edu.thu.tsfile.spark.common.SingleQuery;
import cn.edu.thu.tsfile.spark.common.TSQueryPlan;
import cn.edu.thu.tsfile.spark.exception.QueryOperatorException;
import cn.edu.thu.tsfile.spark.exception.QueryProcessorException;
import cn.edu.thu.tsfile.spark.optimizer.DNFFilterOptimizer;
import cn.edu.thu.tsfile.spark.optimizer.MergeSingleFilterOptimizer;
import cn.edu.thu.tsfile.spark.optimizer.PhysicalOptimizer;
import cn.edu.thu.tsfile.spark.optimizer.RemoveNotOptimizer;
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * This class is used to convert information given by sparkSQL to construct TSFile's query plans.
 * For TSFile's schema differ from SparkSQL's table schema
 * e.g.
 * TSFile's SQL: select s1,s2 from root.car.d1 where s1 = 10
 * SparkSQL's SQL: select s1,s2 from XXX where delta_object = d1
 *
 * @author QJL、KR
 */
public class QueryProcessor {

    //construct logical query plans first, then convert them to physical ones
    public List<TSQueryPlan> generatePlans(FilterOperator filter, List<String> paths, TSRandomAccessFileReader in, Long start, Long end) throws QueryProcessorException, IOException {

        List<TSQueryPlan> queryPlans = new ArrayList<>();

        if(filter != null) {
            RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
            filter = removeNot.optimize(filter);

            DNFFilterOptimizer dnf = new DNFFilterOptimizer();
            filter = dnf.optimize(filter);

            MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
            filter = merge.optimize(filter);

            List<FilterOperator> filterOperators = splitFilter(filter);

            for (FilterOperator filterOperator : filterOperators) {
                SingleQuery singleQuery = constructSelectPlan(filterOperator);
                if (singleQuery != null) {
                    queryPlans.addAll(new PhysicalOptimizer().optimize(singleQuery, paths, in, start, end));
                }
            }
        } else {
            queryPlans.addAll(new PhysicalOptimizer().optimize(null, paths, in, start, end));
        }
        return queryPlans;
    }

    private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
        if (filterOperator.isSingle() || filterOperator.getTokenIntType() != SQLConstant.KW_OR) {
            List<FilterOperator> ret = new ArrayList<FilterOperator>();
            ret.add(filterOperator);
            return ret;
        }
        // a list of conjunctions linked by or
        return filterOperator.childOperators;
    }

    private SingleQuery constructSelectPlan(FilterOperator filterOperator) throws QueryOperatorException {

        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;
        FilterOperator deltaObjectFilterOperator = null;
        List<FilterOperator> singleFilterList = null;

        if (filterOperator.isSingle()) {
            singleFilterList = new ArrayList<>();
            singleFilterList.add(filterOperator);

        } else if (filterOperator.getTokenIntType() == SQLConstant.KW_AND) {
            // original query plan has been dealt with merge optimizer, thus all nodes with same path have been
            // merged to one node
            singleFilterList = filterOperator.getChildren();
        }

        if(singleFilterList == null) {
            return null;
        }

        List<FilterOperator> valueList = new ArrayList<>();
        for (FilterOperator child : singleFilterList) {
            if (!child.isSingle()) {
                valueList.add(child);
            } else {
                switch (child.getSinglePath()) {
                    case SQLConstant.RESERVED_TIME:
                        if (timeFilter != null) {
                            throw new QueryOperatorException(
                                    "time filter has been specified more than once");
                        }
                        timeFilter = child;
                        break;
                    case SQLConstant.RESERVED_DELTA_OBJECT:
                        if (deltaObjectFilterOperator != null) {
                            throw new QueryOperatorException(
                                    "delta object filter has been specified more than once");
                        }
                        deltaObjectFilterOperator = child;
                        break;
                    default:
                        valueList.add(child);
                        break;
                }
            }
        }

        if (valueList.size() == 1) {
            valueFilter = valueList.get(0);

        } else if (valueList.size() > 1) {
            valueFilter = new FilterOperator(SQLConstant.KW_AND, false);
            valueFilter.childOperators = valueList;
        }

        return new SingleQuery(deltaObjectFilterOperator, timeFilter, valueFilter);
    }

}
