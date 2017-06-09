package cn.edu.thu.tsfile.optimizer;

import cn.edu.thu.tsfile.common.FilterOperator;
import cn.edu.thu.tsfile.exception.DNFOptimizeException;
import cn.edu.thu.tsfile.exception.MergeFilterException;
import cn.edu.thu.tsfile.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
