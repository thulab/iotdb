package cn.edu.tsinghua.tsfile.timeseries.filter.expression;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;

public interface UnaryQueryFilter extends QueryFilter{
    Filter getFilter();
}
