package cn.edu.thu.tsfile.qp.optimizer;

import cn.edu.thu.tsfile.qp.common.FilterOperator;
import cn.edu.thu.tsfile.qp.exception.DNFOptimizeException;
import cn.edu.thu.tsfile.qp.exception.MergeFilterException;
import cn.edu.thu.tsfile.qp.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
