package cn.edu.tsinghua.tsfile.read.filter.factory;

import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.filter.operator.And;
import cn.edu.tsinghua.tsfile.read.filter.operator.Not;
import cn.edu.tsinghua.tsfile.read.filter.operator.Or;

public class FilterFactory {
    public static Filter and(Filter left, Filter right){
        return new And(left, right);
    }

    public static Filter or(Filter left, Filter right){
        return new Or(left, right);
    }

    public static Filter not(Filter filter) {
        return new Not(filter);
    }

}
