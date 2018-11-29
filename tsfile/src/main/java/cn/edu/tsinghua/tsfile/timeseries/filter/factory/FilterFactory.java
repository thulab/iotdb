package cn.edu.tsinghua.tsfile.timeseries.filter.factory;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.Or;

public class FilterFactory {
    public static <T extends Comparable<T>> Filter and(Filter left, Filter right){
        return new And(left, right);
    }

    public static <T extends Comparable<T>> Filter or(Filter left, Filter right){
        return new Or(left, right);
    }

    public static <T extends Comparable<T>> Filter not(Filter filter) {
        return new Not(filter);
    }

}
