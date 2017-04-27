package cn.edu.thu.tsfile.spark.optimizer;

import cn.edu.thu.tsfile.spark.common.FilterOperator;
import cn.edu.thu.tsfile.spark.exception.DNFOptimizeException;
import cn.edu.thu.tsfile.spark.exception.MergeFilterException;
import cn.edu.thu.tsfile.spark.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
